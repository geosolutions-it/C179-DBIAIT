# Generated by Django 3.1.2 on 2020-12-10 09:06

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('scheduler', '0014_freezelayer'),
    ]

    operations = [
        migrations.CreateModel(
            name='Freeze',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ref_year', models.IntegerField()),
                ('notes', models.TextField(blank=True, null=True)),
                ('task', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='scheduler.task')),
            ],
        ),
    ]