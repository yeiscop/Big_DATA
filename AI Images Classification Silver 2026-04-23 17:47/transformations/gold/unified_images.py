from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="ai_images_gold_unified",
    comment="Gold layer: All images unified from silver layer (real and AI-generated)"
)
def unified_images():
    # Read both silver tables
    real_images = spark.read.table("ai_images_silver_real")
    fake_images = spark.read.table("ai_images_silver_fake")
    
    # Union both datasets
    return real_images.unionAll(fake_images)
