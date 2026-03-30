"""
Add tsvector search_vector column, GIN index, and auto-update trigger
to the advert table for PostgreSQL full-text search.
"""

from django.db import migrations


FORWARD_SQL = """
-- Add tsvector column
ALTER TABLE advert ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- Populate existing rows
UPDATE advert SET search_vector =
    setweight(to_tsvector('russian', coalesce(title, '')), 'A') ||
    setweight(to_tsvector('russian', coalesce(text, '')), 'B');

-- Create GIN index
CREATE INDEX IF NOT EXISTS advert_search_vector_idx ON advert USING GIN (search_vector);

-- Trigger function to auto-update search_vector on INSERT/UPDATE
CREATE OR REPLACE FUNCTION advert_search_vector_update() RETURNS trigger AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('russian', coalesce(NEW.title, '')), 'A') ||
        setweight(to_tsvector('russian', coalesce(NEW.text, '')), 'B');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger (drop first to be idempotent)
DROP TRIGGER IF EXISTS advert_search_vector_trigger ON advert;
CREATE TRIGGER advert_search_vector_trigger
    BEFORE INSERT OR UPDATE OF title, text ON advert
    FOR EACH ROW
    EXECUTE FUNCTION advert_search_vector_update();
"""

REVERSE_SQL = """
DROP TRIGGER IF EXISTS advert_search_vector_trigger ON advert;
DROP FUNCTION IF EXISTS advert_search_vector_update();
DROP INDEX IF EXISTS advert_search_vector_idx;
ALTER TABLE advert DROP COLUMN IF EXISTS search_vector;
"""


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0012_advertphoto_thumbnail'),
    ]

    operations = [
        migrations.RunSQL(FORWARD_SQL, REVERSE_SQL),
    ]
