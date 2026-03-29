from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0010_emailcampaign_emaillog'),
    ]

    operations = [
        migrations.RenameIndex(
            model_name='emaillog',
            new_name='email_log_campaig_1c62c2_idx',
            old_name='email_log_campaig_b1c2e3_idx',
        ),
        migrations.RenameIndex(
            model_name='emaillog',
            new_name='email_log_recipie_0d21e5_idx',
            old_name='email_log_recipie_a4d5f6_idx',
        ),
        migrations.AlterField(
            model_name='news',
            name='id',
            field=models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID'),
        ),
    ]
