# Generated by Django 3.1.2 on 2020-11-13 21:35

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scheduler', '0003_alldomains'),
    ]

    operations = [
        migrations.AlterField(
            model_name='geopackage',
            name='name',
            field=models.CharField(max_length=50, unique=True),
        ),
    ]
