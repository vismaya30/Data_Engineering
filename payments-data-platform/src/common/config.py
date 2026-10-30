import argparse
import os
from dataclasses import dataclass
from datetime import date

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    data_root: str = "./data"
    aws_region: str = "eu-west-2"
    pg_host: str = "localhost"
    pg_port: int = 15432
    pg_database: str = "payments"
    pg_user: str = "payments"
    pg_password: str = "payments"
    pipeline_start_date: date = date(2026, 10, 1)
    late_window_days: int = 3
    late_threshold_days: int = 2
    max_reject_ratio: float = 0.05
    generator_seed: int = 42
    transactions_per_day: int = 5000
    merchant_count: int = 60
    shuffle_partitions: int = 8

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            data_root=os.getenv("DATA_ROOT", cls.data_root),
            aws_region=os.getenv("AWS_REGION", cls.aws_region),
            pg_host=os.getenv("PG_HOST", cls.pg_host),
            pg_port=int(os.getenv("PG_PORT", cls.pg_port)),
            pg_database=os.getenv("PG_DATABASE", cls.pg_database),
            pg_user=os.getenv("PG_USER", cls.pg_user),
            pg_password=os.getenv("PG_PASSWORD", cls.pg_password),
            pipeline_start_date=date.fromisoformat(
                os.getenv("PIPELINE_START_DATE", cls.pipeline_start_date.isoformat())
            ),
            late_window_days=int(os.getenv("LATE_WINDOW_DAYS", cls.late_window_days)),
            late_threshold_days=int(os.getenv("LATE_THRESHOLD_DAYS", cls.late_threshold_days)),
            max_reject_ratio=float(os.getenv("MAX_REJECT_RATIO", cls.max_reject_ratio)),
            generator_seed=int(os.getenv("GENERATOR_SEED", cls.generator_seed)),
            transactions_per_day=int(os.getenv("TRANSACTIONS_PER_DAY", cls.transactions_per_day)),
            merchant_count=int(os.getenv("MERCHANT_COUNT", cls.merchant_count)),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", cls.shuffle_partitions)),
        )

    @property
    def is_s3(self) -> bool:
        return self.data_root.startswith(("s3://", "s3a://"))

    def path(self, *parts: str) -> str:
        return "/".join([self.data_root.rstrip("/"), *parts])


def run_date_arg(description: str) -> date:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--run-date", required=True, type=date.fromisoformat)
    return parser.parse_args().run_date
