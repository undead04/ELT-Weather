import logging
from typing import Optional

# Lưu ý: Có thể bỏ import LOG_DIR nếu không dùng đến nữa
# từ .config import LOG_DIR 

def setup_logging(level: str = "INFO"):
    """Cấu hình root logger chỉ in ra console (màn hình).
    
    Trong môi trường Docker/Airflow, ghi ra console là cách tốt nhất
    để tránh lỗi quyền hạn và xem log trực tiếp trên UI.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(log_level)

    # Tránh thêm handler nhiều lần
    if any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        return

    # Console handler (In ra màn hình)
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s - %(message)s')
    ch.setFormatter(fmt)
    root.addHandler(ch)


def get_logger(name: Optional[str] = None, domain_file: Optional[str] = None, level: str = "INFO") -> logging.Logger:
    """Trả về một child logger. 
    
    Bỏ qua domain_file vì trong Docker chúng ta không ghi log ra file 
    để tránh lỗi Permission Error.
    """
    if name is None:
        name = "app"

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Trong Docker, chúng ta không cần kiểm tra mkdir LOG_DIR nữa
    # vì không có nhu cầu ghi file log.

    return logger

__all__ = ["setup_logging", "get_logger"]