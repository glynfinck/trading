from prefect_github import GitHubCredentials
from prefect.runner.storage import GitRepository
from . import triangular_arbitrage

if __name__ == "__main__":
    source = GitRepository(
        url="https://github.com/glynfinck/trading.git",
        credentials=GitHubCredentials.load("github-credentials"),
        branch="main"
    )
    triangular_arbitrage.from_source(
        source=source, 
        entrypoint="triangular_arbitrage.py:triangular_arbitrage") \
    .deploy(
        name="triangular-arbitrage",
        work_pool_name="default",
        interval=20
    )