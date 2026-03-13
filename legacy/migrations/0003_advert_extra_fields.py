from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0002_catalog_sort'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql="""
                    ALTER TABLE advert
                        ADD COLUMN IF NOT EXISTS price_unit VARCHAR(10) NOT NULL DEFAULT 'кг',
                        ADD COLUMN IF NOT EXISTS hidden_at TIMESTAMPTZ,
                        ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
                    """,
                    reverse_sql="""
                    ALTER TABLE advert
                        DROP COLUMN IF EXISTS price_unit,
                        DROP COLUMN IF EXISTS hidden_at,
                        DROP COLUMN IF EXISTS deleted_at;
                    """,
                )
            ],
            state_operations=[],
        ),
    ]
