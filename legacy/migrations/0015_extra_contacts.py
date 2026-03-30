from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0014_favorite_advertview'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            ALTER TABLE advert
                ADD COLUMN IF NOT EXISTS extra_contacts jsonb DEFAULT '[]'::jsonb;
            ALTER TABLE legacy_user
                ADD COLUMN IF NOT EXISTS extra_contacts jsonb DEFAULT '[]'::jsonb;
            """,
            reverse_sql="""
            ALTER TABLE advert DROP COLUMN IF EXISTS extra_contacts;
            ALTER TABLE legacy_user DROP COLUMN IF EXISTS extra_contacts;
            """,
        ),
    ]
