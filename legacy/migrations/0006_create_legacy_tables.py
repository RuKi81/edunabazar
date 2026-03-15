from django.db import migrations


class Migration(migrations.Migration):
    """Legacy tables SQL moved to 0001_initial. This migration is now a no-op."""

    dependencies = [
        ('legacy', '0005_message_model'),
    ]

    operations = [
        # SQL was moved to 0001_initial to run before AdvertPhoto FK creation
    ]
