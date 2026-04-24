from pyspark import pipelines as dp

@dp.materialized_view(
    name="ai_images_silver_real", 
    comment="Silver layer: Real (non-AI generated) images filtered from bronze layer"

)
def real_images():
    return (
        spark.read.table("workspace.bronze.ai_images_raw")
        .filter("label = 'REAL'")
    )