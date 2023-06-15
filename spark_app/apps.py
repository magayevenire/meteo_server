from django.apps import AppConfig

class SparkAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'spark_app'

    def ready(self):
       
        print('ready')
        from spark_app.tasks import spark_meteo
        from apscheduler.schedulers.background import BackgroundScheduler
        sm = spark_meteo()

        sm.init_spark()

        scheduler = BackgroundScheduler()
        scheduler.add_job(sm.fetch_data_from_api, trigger='interval', seconds=60)
        scheduler.start()