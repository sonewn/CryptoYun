import os
import time
import requests
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path

class UpbitCollectorQuot:
    def __init__(self, config_path: str = "../../config/api_keys.yaml"):
        self.config = self._load_config(config_path)
        self.base_url = self.config["upbit"]["base_url"]

    def _load_config(self, path: str):
        with open(path, "r") as f:
            return yaml.safe_load(f)
    
    def get_pair_list(self, is_details: str):
        "업비트 지원 페어 목록 조회"
        url = f"{self.base_url}/market/all?is_details={is_details}"

        headers = {"accept": "application/json"}

        res = requests.get(url, headers=headers)
        return pd.DataFrame(res.json())

    def save_file(self, df:pd.DataFrame, category: str):
        "수집 데이터 파일 저장 - 추후 확장자 고민 필요"
        base_dir = Path(__file__).resolve().parent.parent.parent
        data_dir = base_dir / "data" / "raw" / "quot" / category
        data_dir.mkdir(parents=True, exist_ok=True)

        file_path = data_dir / f"pairs_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(file_path, index=False)
        print(f"Saved {category} data succesfully --> {file_path}")



if __name__ == "__main__":
    collector = UpbitCollectorQuot()

    # 1️⃣ 전체 페어 목록 조회
    pairs_df = collector.get_pair_list("true")
    collector.save_file(pairs_df, "pairs_list")