"""
github_sync.py — GitHub 仓库同步（用于 bucket 数据云端备份）

策略：
- 同步 buckets_dir 下的 .md 记忆文件
- embeddings.db 不上传（可重建）
- 使用 GitHub Git Trees API 批量提交（一次同步 = 一个 commit）
- 支持手动触发 + 可选的定时自动同步
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import tempfile
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("ombre_brain.github_sync")

_API = "https://api.github.com"
_TIMEOUT = 60.0
_MAX_FILE_BYTES = 2 * 1024 * 1024
_TREE_CHUNK = 200
_TREE_CHUNK_BYTES = 2 * 1024 * 1024
_MAX_BACKUP_FILES = 10_000
_MAX_BACKUP_PATH_BYTES = 1024
_MAX_MANIFEST_PAYLOAD_BYTES = 4 * 1024 * 1024
_MAX_MANIFEST_BASE64_BYTES = ((_MAX_MANIFEST_PAYLOAD_BYTES + 2) // 3) * 4 + 64 * 1024
_MAX_RESTORE_TOTAL_BYTES = 512 * 1024 * 1024
_MANIFEST_FILENAME = "_ombre_backup_manifest.json"


class _LazyMarkdownFiles(Mapping[str, bytes]):
    def __init__(self, paths: Mapping[str, str]) -> None:
        self._paths = dict(paths)

    def __len__(self) -> int:
        return len(self._paths)

    def __iter__(self) -> Iterator[str]:
        return iter(self._paths)

    def __getitem__(self, relative_path: str) -> bytes:
        full_path = self._paths[relative_path]
        if os.path.islink(full_path):
            raise RuntimeError(f"GitHub backup refuses symbolic-link file: {relative_path}")
        size = os.path.getsize(full_path)
        if size > _MAX_FILE_BYTES:
            raise RuntimeError(f"GitHub backup file too large: {relative_path} ({size} bytes)")
        with open(full_path, "rb") as handle:
            content = handle.read(_MAX_FILE_BYTES + 1)
        if len(content) > _MAX_FILE_BYTES:
            raise RuntimeError(f"GitHub backup file too large: {relative_path}")
        return content


def _iter_backup_paths(root_dir: str) -> Iterator[str]:
    iterators: list[os.ScandirIterator] = []
    try:
        try:
            iterators.append(os.scandir(root_dir))
        except OSError as exc:
            logger.warning(f"[github_sync] cannot scan {root_dir}: {exc}")
            return
        while iterators:
            current = iterators[-1]
            try:
                entry = next(current)
            except StopIteration:
                current.close()
                iterators.pop()
                continue
            except OSError as exc:
                logger.warning(f"[github_sync] directory scan failed: {exc}")
                current.close()
                iterators.pop()
                continue
            try:
                if entry.is_dir(follow_symlinks=False):
                    try:
                        iterators.append(os.scandir(entry.path))
                    except OSError as exc:
                        logger.warning(f"[github_sync] cannot scan {entry.path}: {exc}")
                    continue
                if entry.is_file(follow_symlinks=False) and entry.name.endswith(".md"):
                    yield entry.path
            except OSError as exc:
                logger.warning(f"[github_sync] skip {entry.path}: {exc}")
    finally:
        for iterator in reversed(iterators):
            iterator.close()


class GitHubSync:
    """向 GitHub 仓库批量上传记忆。"""

    def __init__(
        self,
        token: str,
        repo: str,
        branch: str = "main",
        path_prefix: str = "ombre",
    ):
        self.token = token
        self.repo = repo.strip()
        self.branch = branch.strip() or "main"
        self.path_prefix = path_prefix.strip().strip("/")
        self._headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.last_sync: str | None = None
        self.last_status: str = "idle"
        self.last_error: str = ""
        self.last_count: int = 0
        self.is_validated: bool = False
        self.consecutive_failures: int = 0
        self._sync_lock = asyncio.Lock()

    async def sync(self, buckets_dir: str) -> dict[str, Any]:
        async with self._sync_lock:
            try:
                files = self._collect_files(buckets_dir)
                if not files:
                    self.last_status = "ok"
                    self.last_error = ""
                    self.last_sync = _now_iso()
                    self.last_count = 0
                    return {"ok": True, "uploaded": 0, "message": "无可同步文件"}
                count = await self._batch_commit(files)
                self.last_sync = _now_iso()
                self.last_status = "ok"
                self.last_error = ""
                self.last_count = count
                self.consecutive_failures = 0
                return {"ok": True, "uploaded": count}
            except Exception as e:
                self.last_status = "error"
                self.last_error = str(e)
                self.consecutive_failures += 1
                logger.error(f"[github_sync] sync failed (连续 {self.consecutive_failures} 次): {e}")
                return {"ok": False, "error": str(e)}

    async def import_from_github(self, buckets_dir: str) -> dict[str, Any]:
        async with self._sync_lock:
            return await self._import_locked(buckets_dir)

    async def _import_locked(self, buckets_dir: str) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(headers=self._headers, timeout=_TIMEOUT) as c:
                r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/ref/heads/{self.branch}")
                if _is_empty_repo_response(r):
                    return {"ok": True, "imported": 0, "message": "GitHub 仓库为空"}
                if r.status_code == 404:
                    return {"ok": False, "error": f"分支 {self.branch} 不存在"}
                r.raise_for_status()
                head_sha = r.json()["object"]["sha"]
                r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/commits/{head_sha}")
                r.raise_for_status()
                tree_sha = r.json()["tree"]["sha"]
                r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/trees/{tree_sha}?recursive=1")
                r.raise_for_status()
                tj = r.json()
                tree = tj.get("tree", [])
                if bool(tj.get("truncated")):
                    raise RuntimeError("GitHub returned a truncated tree")

                prefix = (self.path_prefix + "/") if self.path_prefix else ""
                targets = [
                    t for t in tree
                    if t.get("type") == "blob"
                    and str(t.get("path", "")).startswith(prefix)
                    and str(t.get("path", "")).endswith(".md")
                ]
                if not targets:
                    return {"ok": True, "imported": 0, "message": "没有可恢复的记忆文件"}

                base = os.path.abspath(buckets_dir)
                imported = 0
                skipped = 0
                errors: list[str] = []

                with tempfile.TemporaryDirectory(prefix="ombre-github-restore-") as staging_dir:
                    staged: dict[str, str] = {}
                    restored_bytes = 0
                    for index, t in enumerate(targets):
                        rel = t["path"][len(prefix):]
                        if not rel:
                            continue
                        dest = os.path.abspath(os.path.join(base, rel))
                        if dest != base and not dest.startswith(base + os.sep):
                            raise RuntimeError(f"{rel}: path escapes the vault")
                        rb = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/blobs/{t['sha']}")
                        rb.raise_for_status()
                        bj = rb.json()
                        if bj.get("encoding") == "base64":
                            encoded = "".join(str(bj.get("content", "") or "").split())
                            data = base64.b64decode(encoded, validate=True)
                        else:
                            data = (bj.get("content", "") or "").encode("utf-8")
                        if len(data) > _MAX_FILE_BYTES:
                            raise RuntimeError(f"decoded file exceeds limit")
                        restored_bytes += len(data)
                        if restored_bytes > _MAX_RESTORE_TOTAL_BYTES:
                            raise RuntimeError("restore exceeds total limit")
                        stage_path = os.path.join(staging_dir, f"{index:05d}.blob")
                        with open(stage_path, "wb") as sf:
                            sf.write(data)
                        staged[rel] = stage_path

                    for rel, stage_path in sorted(staged.items()):
                        dest = os.path.abspath(os.path.join(base, rel))
                        try:
                            os.makedirs(os.path.dirname(dest), exist_ok=True)
                            temp_path = f"{dest}.{uuid.uuid4().hex}.tmp"
                            with open(stage_path, "rb") as sf:
                                data = sf.read()
                            with open(temp_path, "wb") as out:
                                out.write(data)
                                out.flush()
                                os.fsync(out.fileno())
                            os.replace(temp_path, dest)
                            imported += 1
                        except Exception as exc:
                            skipped += 1
                            errors.append(f"{rel}: {exc}")

                self.last_sync = _now_iso()
                return {
                    "ok": skipped == 0,
                    "imported": imported,
                    "skipped": skipped,
                    "total": len(targets),
                    "errors": errors[:10],
                }
        except Exception as e:
            logger.error(f"[github_sync] import failed: {e}")
            self.last_status = "error"
            self.last_error = str(e)
            return {"ok": False, "error": str(e)}

    async def validate(self) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(headers=self._headers, timeout=15.0) as c:
                r = await c.get(f"{_API}/repos/{self.repo}")
                if r.status_code == 404:
                    return {"ok": False, "error": f"仓库 {self.repo} 不存在或无权限访问"}
                if r.status_code == 401:
                    return {"ok": False, "error": "Token 无效或已过期"}
                r.raise_for_status()
                data = r.json()
                perms = data.get("permissions", {})
                can_push = perms.get("push", False) or perms.get("admin", False)
                if perms and not can_push:
                    return {"ok": False, "error": "Token 只有读权限"}
                self.is_validated = True
                return {
                    "ok": True,
                    "repo_full_name": data.get("full_name", self.repo),
                    "private": data.get("private", False),
                    "default_branch": data.get("default_branch", "main"),
                    "can_push": can_push,
                }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def status(self) -> dict[str, Any]:
        return {
            "enabled": bool(self.token and self.repo),
            "repo": self.repo,
            "branch": self.branch,
            "path_prefix": self.path_prefix,
            "last_sync": self.last_sync,
            "last_status": self.last_status,
            "last_error": self.last_error,
            "last_count": self.last_count,
            "is_validated": self.is_validated,
            "consecutive_failures": self.consecutive_failures,
        }

    def _collect_files(self, buckets_dir: str) -> Mapping[str, bytes]:
        paths: dict[str, str] = {}
        if not os.path.isdir(buckets_dir):
            return _LazyMarkdownFiles(paths)
        base_real = os.path.realpath(buckets_dir)
        for full in _iter_backup_paths(buckets_dir):
            try:
                full_real = os.path.realpath(full)
                if os.path.commonpath((base_real, full_real)) != base_real:
                    continue
                size = os.path.getsize(full)
                if size > _MAX_FILE_BYTES:
                    continue
                relative = os.path.relpath(full, buckets_dir).replace("\\", "/")
                if len(relative.encode("utf-8")) > _MAX_BACKUP_PATH_BYTES:
                    continue
                if relative not in paths and len(paths) >= _MAX_BACKUP_FILES:
                    raise RuntimeError(f"Too many files (>{_MAX_BACKUP_FILES})")
                paths[relative] = full
            except OSError as e:
                logger.warning(f"[github_sync] skip: {e}")
        return _LazyMarkdownFiles(paths)

    async def _batch_commit(self, files: Mapping[str, bytes]) -> int:
        async with httpx.AsyncClient(headers=self._headers, timeout=_TIMEOUT) as c:
            r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/ref/heads/{self.branch}")
            bootstrap_branch = _is_empty_repo_response(r)
            head_sha: str | None = None
            base_tree_sha: str | None = None
            if r.status_code == 404:
                raise RuntimeError(f"分支 {self.branch} 不存在")
            if not bootstrap_branch:
                r.raise_for_status()
                head_sha = r.json()["object"]["sha"]
                r = await self._request(c, "GET", f"{_API}/repos/{self.repo}/git/commits/{head_sha}")
                r.raise_for_status()
                base_tree_sha = r.json()["tree"]["sha"]

            manifest = self._build_manifest(files)
            manifest_content = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True)
            manifest_path = f"{self.path_prefix}/{_MANIFEST_FILENAME}" if self.path_prefix else _MANIFEST_FILENAME

            cur_base = base_tree_sha
            for file_chunk in self._iter_chunks(files, manifest):
                tree_entries: list[dict[str, Any]] = []
                for rel_path, content in file_chunk:
                    gh_path = f"{self.path_prefix}/{rel_path}" if self.path_prefix else rel_path
                    try:
                        text = content.decode("utf-8")
                        entry = {"path": gh_path, "mode": "100644", "type": "blob", "content": text}
                    except UnicodeDecodeError:
                        rb = await self._request(
                            c, "POST", f"{_API}/repos/{self.repo}/git/blobs",
                            json={"content": base64.b64encode(content).decode(), "encoding": "base64"},
                        )
                        rb.raise_for_status()
                        entry = {"path": gh_path, "mode": "100644", "type": "blob", "sha": rb.json()["sha"]}
                    tree_entries.append(entry)
                tree_payload: dict[str, Any] = {"tree": tree_entries}
                if cur_base:
                    tree_payload["base_tree"] = cur_base
                r = await self._request(c, "POST", f"{_API}/repos/{self.repo}/git/trees", json=tree_payload)
                r.raise_for_status()
                cur_base = r.json()["sha"]
                del r, tree_payload, tree_entries, file_chunk

            manifest_entry = {"path": manifest_path, "mode": "100644", "type": "blob", "content": manifest_content}
            manifest_payload: dict[str, Any] = {"tree": [manifest_entry]}
            if cur_base:
                manifest_payload["base_tree"] = cur_base
            r = await self._request(c, "POST", f"{_API}/repos/{self.repo}/git/trees", json=manifest_payload)
            r.raise_for_status()
            new_tree_sha = r.json()["sha"]

            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            r = await self._request(
                c, "POST", f"{_API}/repos/{self.repo}/git/commits",
                json={
                    "message": f"Ombre Brain sync — {now_str} ({len(files)} files)",
                    "tree": new_tree_sha,
                    "parents": [head_sha] if head_sha else [],
                },
            )
            r.raise_for_status()
            commit_sha: str = r.json()["sha"]

            if bootstrap_branch:
                r = await self._request(
                    c, "POST", f"{_API}/repos/{self.repo}/git/refs",
                    json={"ref": f"refs/heads/{self.branch}", "sha": commit_sha},
                )
            else:
                r = await self._request(
                    c, "PATCH", f"{_API}/repos/{self.repo}/git/refs/heads/{self.branch}",
                    json={"sha": commit_sha, "force": False},
                )
            r.raise_for_status()

        return len(files)

    def _build_manifest(self, files: Mapping[str, bytes]) -> dict[str, Any]:
        entries = []
        total_bytes = 0
        for rel_path in sorted(files):
            content = files[rel_path]
            size = len(content)
            total_bytes += size
            entries.append({
                "path": rel_path,
                "bytes": size,
                "sha256": hashlib.sha256(content).hexdigest(),
            })
        return {
            "schema_version": 1,
            "source": "ombre-brain",
            "generated_at": _now_iso(),
            "repo": self.repo,
            "branch": self.branch,
            "path_prefix": self.path_prefix,
            "file_count": len(entries),
            "total_bytes": total_bytes,
            "files": entries,
        }

    def _iter_chunks(self, files: Mapping[str, bytes], manifest: dict) -> Iterator[list[tuple[str, bytes]]]:
        chunk: list[tuple[str, bytes]] = []
        chunk_bytes = 0
        for entry in manifest.get("files", []):
            rel_path = str(entry["path"])
            content = files[rel_path]
            if not isinstance(content, bytes):
                content = bytes(content)
            if chunk and (len(chunk) >= _TREE_CHUNK or chunk_bytes + len(content) > _TREE_CHUNK_BYTES):
                yield chunk
                chunk = []
                chunk_bytes = 0
            chunk.append((rel_path, content))
            chunk_bytes += len(content)
        if chunk:
            yield chunk

    async def _request(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        *,
        json: dict | None = None,
        _max_retries: int = 4,
    ) -> httpx.Response:
        for attempt in range(_max_retries + 1):
            resp = await client.request(method, url, json=json)
            if resp.status_code not in (403, 429):
                return resp
            body_l = resp.text.lower()
            is_rate = (
                "rate limit" in body_l
                or "retry-after" in {k.lower() for k in resp.headers}
                or resp.headers.get("x-ratelimit-remaining") == "0"
            )
            if not is_rate or attempt == _max_retries:
                return resp
            retry_after = resp.headers.get("retry-after")
            if retry_after and retry_after.isdigit():
                wait = int(retry_after)
            else:
                wait = min(2 ** attempt, 30)
            logger.warning(f"[github_sync] rate limited, retry in {wait}s (attempt {attempt + 1})")
            await asyncio.sleep(wait)
        return resp


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _is_empty_repo_response(resp: httpx.Response) -> bool:
    if resp.status_code != 409:
        return False
    try:
        message = str(resp.json().get("message", ""))
    except Exception:
        message = resp.text
    return "empty" in message.lower()
