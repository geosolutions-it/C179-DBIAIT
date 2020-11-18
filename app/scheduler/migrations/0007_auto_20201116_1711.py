# Generated by Django 3.1.2 on 2020-11-16 16:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scheduler', '0006_auto_20201116_1643'),
    ]

    operations = [
        migrations.RenameField(
            model_name='importedlayer',
            old_name='import_end_date',
            new_name='import_start_timestamp',
        ),
        migrations.RemoveField(
            model_name='importedlayer',
            name='import_start_date',
        ),
        migrations.AddField(
            model_name='importedlayer',
            name='import_end_timestamp',
            field=models.DateTimeField(null=True),
        ),
    ]