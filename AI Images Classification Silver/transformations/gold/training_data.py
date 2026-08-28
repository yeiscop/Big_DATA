from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="ai_images_gold_training",
    comment="Gold layer: Training dataset (80% split) for AI image classification model"
)
def training_data():
    return (
        spark.read.table("ai_images_gold_unified")
        .filter((F.hash(F.col("file_path")) % 10) < 8)
    )
