from celery import Celery

# Create a Celery instance
app = Celery('news_processing', broker='redis://localhost:6379/0')

# Optional configuration
app.conf.task_routes = {
    'process_article': {'queue': 'article_processing_queue'},
}
