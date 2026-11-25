# 08 · pgvector 安装指南（PostgreSQL 14 / Ubuntu 22.04）

> 当前环境（2026-11-22 已验证）：
>
> - pgvector 0.8.7 已源码安装
> - `CREATE EXTENSION vector` 成功
> - `SELECT '[1,2,3]'::vector` 返回 `[1,2,3]`
>
> 以下保留完整安装步骤，方便换环境时复现。
>
> - PostgreSQL 14.24（Ubuntu 包）
> - `pg_config --sharedir = /usr/share/postgresql/14`
> - `postgresql-server-dev-14` 已安装
> - gcc / make / git 已安装
> - 默认 apt 源没有 `postgresql-14-pgvector`
> - GitHub codeload 可访问，`v0.8.7` tarball 返回 200
> - PGDG 源有 `postgresql-14-pgvector 0.8.7`，但依赖 `postgresql-14 (>> 14.25)`

## 方案 A：源码编译（推荐）

不用改系统包源，也不会升级 PostgreSQL。

### 1. 下载源码

```bash
mkdir -p ~/src
cd ~/src

curl -L --max-time 120 \
  -o pgvector-0.8.7.tar.gz \
  https://codeload.github.com/pgvector/pgvector/tar.gz/refs/tags/v0.8.7

tar -xzf pgvector-0.8.7.tar.gz
cd pgvector-0.8.7
```

如果之前下过一半的文件，重新执行上面的 `curl` 覆盖即可。

### 2. 编译

关键是用 PostgreSQL 14 的 `pg_config`，不要用错版本：

```bash
PG_CONFIG=$(pg_config --bindir)/pg_config
echo "$PG_CONFIG"
make PG_CONFIG="$PG_CONFIG"
```

如果之前编译失败过，先清理：

```bash
make clean PG_CONFIG="$PG_CONFIG"
```

### 3. 安装扩展

```bash
sudo make install PG_CONFIG="$PG_CONFIG"
```

验证文件是否装到 PostgreSQL 14 的目录：

```bash
ls "$(pg_config --sharedir)/extension/vector.control"
ls "$(pg_config --pkglibdir)/vector.so"
```

> 注意：`pg_config --libdir` 是系统库目录（`/usr/lib/x86_64-linux-gnu`），PostgreSQL 扩展的 `.so` 在 `pg_config --pkglibdir`（当前是 `/usr/lib/postgresql/14/lib`）。

### 4. 在数据库里启用

```bash
sudo -u postgres psql -d ai_companion \
  -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

验证：

```bash
sudo -u postgres psql -d ai_companion \
  -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'vector';"

sudo -u postgres psql -d ai_companion \
  -c "SELECT '[1,2,3]'::vector;"
```

期望：

```text
 vector | 0.8.7

 [1,2,3]
```

> `CREATE EXTENSION` 需要超级用户。当前应用连接的是 `postgres`，所以 P4 迁移里可以直接建扩展。以后换成普通用户时，需要 DBA 先执行一次。

## 方案 B：PGDG apt 包（可选，改动更大）

PGDG 源确实有包：

```text
Package: postgresql-14-pgvector
Version: 0.8.7-1.pgdg22.04+2
Depends: postgresql-14 (>> 14.25)
```

你当前是 14.24，安装它可能会把 PostgreSQL 从 Ubuntu 包升级到 PGDG 的 14.25+。执行前先看清楚：

```bash
sudo apt install -y curl ca-certificates lsb-release
sudo install -d /usr/share/postgresql-common/pgdg

sudo curl -fsSL \
  -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
  https://www.postgresql.org/media/keys/ACCC4CF8.asc

echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list

sudo apt update
apt-cache policy postgresql-14 postgresql-14-pgvector
```

确认会升级哪些包后，再：

```bash
sudo apt install -y postgresql-14-pgvector
```

如果不想动现有 PostgreSQL，优先用方案 A。

## 方案 C：Docker（不推荐）

`pgvector/pgvector:pg16` 镜像本身可用，但 Docker Hub 在当前网络下拉取超时；除非配置了可用的镜像/代理，否则不作为主路径。

## 常见错误

| 错误 | 原因 | 解决 |
|---|---|---|
| `E: Unable to locate package postgresql-14-pgvector` | Ubuntu 默认源没有 | 用方案 A，或加 PGDG 源 |
| `git clone` 超时 | GitHub 克隆慢 | 用 codeload tarball（方案 A） |
| `pg_config: command not found` | dev 包没装 | `sudo apt install postgresql-server-dev-14` |
| `fatal error: postgres.h: No such file` | 头文件没装 | 同上 |
| `make: *** No targets specified` | 不在 pgvector 源码目录 | `cd pgvector-0.8.7` |
| `Permission denied` | 安装目录需要 root | `sudo make install` |
| `could not open extension control file` | 装到了别的 PG 版本目录 | 检查 `pg_config --sharedir`，用同一个 `PG_CONFIG` 重装 |
| `permission denied to create extension` | 当前 DB 用户不是超级用户 | 用 `sudo -u postgres psql` 执行 |
| `vector.so: cannot open shared object file` | 扩展库目录不匹配/权限问题 | 检查 `pg_config --pkglibdir`，重装 |

## 装完后

P4 迁移 `0004_long_term_memory` 会包含：

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

以及 `memories` / `documents` / `document_chunks` 和 HNSW 索引。

如果安装过程中报错，把下面命令的输出贴出来：

```bash
pg_config --version
pg_config --sharedir
pg_config --pkglibdir
ls -l "$(pg_config --sharedir)/extension/" | grep vector || true
ls -l "$(pg_config --pkglibdir)/" | grep vector || true
sudo -u postgres psql -d ai_companion -c "CREATE EXTENSION IF NOT EXISTS vector;"
```
