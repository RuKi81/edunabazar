from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0001_initial'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql="""
                    ALTER TABLE catalog
                    ADD COLUMN IF NOT EXISTS sort integer NOT NULL DEFAULT 0;
                    """,
                    reverse_sql="""
                    ALTER TABLE catalog
                    DROP COLUMN IF EXISTS sort;
                    """,
                )
            ],
            state_operations=[],
        ),
    ]
