# Generated by Django 3.1.2 on 2020-11-18 11:21

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scheduler', '0012_remove_importedlayer_status'),
    ]

    operations = [
        migrations.AddField(
            model_name='importedlayer',
            name='status',
            field=models.CharField(default='QUEUED', max_length=20),
        ),
    ]