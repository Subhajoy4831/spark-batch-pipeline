from diagrams import Diagram, Cluster
from diagrams.aws.storage import S3
from diagrams.aws.analytics import Glue, Athena
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.ci import GithubActions
from diagrams.onprem.analytics import Spark

with Diagram(
    "AWS Batch Data Pipeline",
    filename="docs/architecture",
    outformat="png",
    show=False,
    direction="TB",
    graph_attr={"fontsize": "18", "pad": "0.5", "bgcolor": "white"},
):
    with Cluster("CI / CD"):
        ci = GithubActions("GitHub Actions\nlint · test · build · integrate")

    with Cluster("Docker Compose"):
        scheduler  = Airflow("Scheduler")
        webserver  = Airflow("Webserver")
        postgres   = PostgreSQL("Metadata DB")
        scheduler - postgres
        webserver  - postgres

    with Cluster("AWS"):
        with Cluster("S3"):
            s3_raw     = S3("raw/orders/\n(CSV)")
            s3_clean   = S3("cleaned/orders/\n(Parquet)")

        with Cluster("PySpark Pipeline"):
            ingest    = Spark("Ingest")
            validate  = Spark("Validate")
            transform = Spark("Transform")
            load      = Spark("Load")

        glue   = Glue("Glue Catalog")
        athena = Athena("Athena")

    ci >> scheduler
    scheduler >> ingest
    s3_raw >> ingest >> validate >> transform >> load >> s3_clean
    s3_clean >> glue >> athena
