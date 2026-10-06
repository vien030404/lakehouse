"""
Khai báo sẵn kết nối Trino trong Superset (cấu hình dưới dạng code), để UI
dùng được ngay mà không phải thêm database bằng tay.

Chạy trong app context của Superset (xem entrypoint-superset.sh). Idempotent:
chạy lại chỉ cập nhật, không tạo bản trùng.

Kết nối được tạo:
    Trino - Lakehouse (Iceberg)   catalog lakehouse  <- dùng cho dashboard

Script cũng dọn các kết nối trỏ tới catalog không còn tồn tại trên Trino
(danh sách OBSOLETE). Kết nối nào đã có dataset sử dụng thì được giữ lại kèm
cảnh báo, vì xoá sẽ làm hỏng dashboard.
"""

import json
import os
import sys

HOST = os.environ.get("TRINO_HOST", "trino-coordinator")
PORT = os.environ.get("TRINO_PORT", "8080")
USER = os.environ.get("TRINO_USER", "admin")

DATABASES = [
    ("Trino - Lakehouse (Iceberg)", "lakehouse"),
]

# Kết nối tới các catalog không còn được cấu hình trên Trino
OBSOLETE = [
    "Trino - PostgreSQL (ops)",
    "Trino - MySQL (shop)",
]


def main() -> int:
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.core import Database

        for name, catalog in DATABASES:
            uri = f"trino://{USER}@{HOST}:{PORT}/{catalog}"

            database = db.session.query(Database).filter_by(database_name=name).one_or_none()
            created = database is None
            if created:
                database = Database(database_name=name)
                db.session.add(database)

            database.sqlalchemy_uri = uri
            database.expose_in_sqllab = True
            database.allow_ctas = True
            database.allow_cvas = True
            database.allow_dml = True
            database.allow_run_async = False        # không có Celery/Redis
            database.cache_timeout = 300
            database.extra = json.dumps(
                {
                    "metadata_params": {},
                    "engine_params": {
                        # Trino không bật auth, danh tính lấy từ user trong URI
                        "connect_args": {"http_scheme": "http"}
                    },
                    "schemas_allowed_for_file_upload": [],
                }
            )

            print(f"  {'tạo mới' if created else 'cập nhật'}: {name} -> {uri}")

        from superset.connectors.sqla.models import SqlaTable

        for name in OBSOLETE:
            database = db.session.query(Database).filter_by(database_name=name).one_or_none()
            if database is None:
                continue
            used = db.session.query(SqlaTable).filter_by(database_id=database.id).count()
            if used:
                print(f"  GIỮ LẠI: {name} - còn {used} dataset dùng, xoá tay sau khi chuyển dataset")
                continue
            db.session.delete(database)
            print(f"  xoá: {name} (catalog đã gỡ khỏi Trino)")

        db.session.commit()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # pragma: no cover
        # Lỗi bootstrap không được làm dừng container: Superset vẫn chạy, kết
        # nối có thể thêm bằng tay.
        print(f"  CẢNH BÁO: không tạo được kết nối Trino tự động: {exc}")
        sys.exit(0)
