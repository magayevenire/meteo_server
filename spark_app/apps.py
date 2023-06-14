from django.apps import AppConfig


class SparkAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'spark_app'

    def ready(self):
       
        from spark_app import tasks
        # tasks.start()