import os
import time
import requests
import jwt
import yaml
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import uuid
from pathlib import Path

class UpbitCollector:
    def __init__(self, config_path: str = "../../config/api_keys.yaml"):
        self.config = self._load_config(config_path)
        self.access_key = self.config["upbit"]["access_key"]
        self.secret_key = self.config["upbit"]["secret_key"]
        self.base_url = self.config["upbit"]["base_url"]

    def _load_config(self, path: str):
        with open(path, "r") as f:
            return yaml.safe_load(f)
    
    def _create_headers(self, query: Optional[dict] = None):
        payload = {
            "access_key": self.access_key,
            "nonce": str(uuid.uuid4()),
        }

        if query != None:
            import hashlib
            import urllib.parse

            query_string = urllib.parse.urlencode(query)
            m = hashlib.sha512()
            m.update(query_string.encode("utf-8"))
            query_hash = m.hexdigest()
            payload["query_hash"] = query_hash
            payload["query_hash_alg"] = "SHA512"
        
        jwt_token = jwt.encode(payload, self.secret_key, algorithm="HS256")
        auth = f"Bearer {jwt_token}"

        return {"Authorization": auth}
    
    def get_my_asset(self):
        "보유 자산 현황 조회"
        url = f"{self.base_url}/accounts"
        headers = self._create_headers()
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        return pd.DataFrame(res.json())
    
    def save_file(self, df:pd.DataFrame, category: str):
        "수집 데이터 파일 저장 - 추후 확장자 고민 필요"
        base_dir = Path(__file__).resolve().parent.parent.parent
        data_dir = base_dir / "data" / "raw" / category
        data_dir.mkdir(parents=True, exist_ok=True)

        file_path = data_dir / f"{self.access_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(file_path, index=False)
        print(f"Saved {category} data succesfully --> {file_path}")


if __name__ == "__main__":
    collector = UpbitCollector()

    # 1️⃣ 나의 자산현황
    balance_df = collector.get_my_asset()
    collector.save_file(balance_df, "my_asset")