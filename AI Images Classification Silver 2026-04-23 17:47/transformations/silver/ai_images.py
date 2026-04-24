from pyspark import pipelines as dp

@dp.materialized_view(
    name="ai_images_silver_fake", # Corregido de 'ame' a 'name'
    comment="Silver layer: AI-generated (fake) images filtered from bronze layer"
)
def ai_images():
    return (
        spark.read.table("workspace.bronze.ai_images_raw")
        .filter("label = 'FAKE'")
    )