import sys
import logging
import argparse
from pyspark.sql import SparkSession

# 1. 配置生產級日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_merge():
    # 2. 解析由 Airflow 或 K8s 傳入的參數
    parser = argparse.ArgumentParser(description='Bank Gold Layer Merge Job')
    parser.add_argument('--source', required=True, help='Source temp view or table name')
    parser.add_argument('--target', required=True, help='Target Gold table name (e.g., gold_top_routes)')
    parser.add_argument('--ref', default='main', help='Nessie reference/branch name')
    args = parser.parse_args()

    # 3. 初始化 SparkSession 
    # 注意：在 K8s 模式下，所有的 .config 已經由 SparkApplication YAML 注入，
    # 這裡只需要呼叫 getOrCreate() 即可拿到具備 Iceberg 能力的 Session。
    spark = SparkSession.builder.getOrCreate()

    try:
        logger.info(f"正在切換 Nessie 分支至: {args.ref}")
        # 告訴 Nessie 我們要在哪個分支上作業
        spark.sql(f"USE REFERENCE {args.ref} IN nessie")

        logger.info(f"開始執行 Merge 操作: {args.source} -> {args.target}")
        
        # 4. 執行核心 MERGE INTO 語法
        # 使用組合鍵 (pickup_zone, dropoff_zone) 來判斷數據是否存在
        merge_sql = f"""
            MERGE INTO nessie.etl_study.{args.target} AS target
            USING {args.source} AS source
            ON target.pickup_zone = source.pickup_zone AND target.dropoff_zone = source.dropoff_zone
            WHEN MATCHED THEN
                UPDATE SET 
                    target.route_revenue = source.route_revenue, 
                    target.rank = source.rank,
                    target.updated_at = current_timestamp()
            WHEN NOT MATCHED THEN
                INSERT (pickup_zone, dropoff_zone, route_revenue, rank, created_at, updated_at)
                VALUES (source.pickup_zone, source.dropoff_zone, source.route_revenue, source.rank, current_timestamp(), current_timestamp())
        """
        
        spark.sql(merge_sql)
        logger.info("Merge 操作成功完成！")

    except Exception as e:
        logger.error(f"任務執行失敗: {str(e)}")
        sys.exit(1) # 回傳非零狀態碼，讓 Airflow/K8s 知道任務失敗並觸發重試
    finally:
        spark.stop()

if __name__ == "__main__":
    run_merge()