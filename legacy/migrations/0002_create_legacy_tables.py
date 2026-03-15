from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('legacy', '0001_initial'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            -- Enable PostGIS
            CREATE EXTENSION IF NOT EXISTS postgis;

            -- legacy_user
            CREATE TABLE IF NOT EXISTS legacy_user (
                id SERIAL PRIMARY KEY,
                type SMALLINT NOT NULL DEFAULT 0,
                username VARCHAR(255) NOT NULL UNIQUE,
                auth_key VARCHAR(32) NOT NULL DEFAULT '',
                password_hash VARCHAR(255) NOT NULL DEFAULT '',
                password_reset_token VARCHAR(255) UNIQUE,
                email VARCHAR(255) NOT NULL UNIQUE,
                currency VARCHAR(5) NOT NULL DEFAULT 'RUB',
                name VARCHAR(255) NOT NULL DEFAULT '',
                address VARCHAR(255) NOT NULL DEFAULT '',
                phone VARCHAR(255) NOT NULL DEFAULT '',
                inn VARCHAR(20) NOT NULL DEFAULT '',
                status SMALLINT NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                location geometry(Point,4326),
                contacts TEXT NOT NULL DEFAULT ''
            );

            -- catalog
            CREATE TABLE IF NOT EXISTS catalog (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL UNIQUE,
                sort INTEGER NOT NULL DEFAULT 0,
                active SMALLINT NOT NULL DEFAULT 1
            );

            -- categories
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                catalog INTEGER NOT NULL REFERENCES catalog(id),
                title VARCHAR(255) NOT NULL UNIQUE,
                active SMALLINT NOT NULL DEFAULT 1
            );

            -- advert
            CREATE TABLE IF NOT EXISTS advert (
                id SERIAL PRIMARY KEY,
                type SMALLINT NOT NULL DEFAULT 0,
                category INTEGER NOT NULL REFERENCES categories(id),
                author INTEGER NOT NULL REFERENCES legacy_user(id),
                address VARCHAR(255),
                location geometry(Point,4326) NOT NULL,
                delivery BOOLEAN NOT NULL DEFAULT FALSE,
                contacts TEXT NOT NULL DEFAULT '',
                title VARCHAR(255) NOT NULL DEFAULT '',
                text TEXT NOT NULL DEFAULT '',
                price DOUBLE PRECISION NOT NULL DEFAULT 0,
                wholesale_price DOUBLE PRECISION NOT NULL DEFAULT 0,
                min_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
                wholesale_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
                volume DOUBLE PRECISION NOT NULL DEFAULT 0,
                priority INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                price_unit VARCHAR(10) DEFAULT 'кг',
                hidden_at TIMESTAMP,
                deleted_at TIMESTAMP,
                status SMALLINT NOT NULL DEFAULT 0
            );

            -- seller
            CREATE TABLE IF NOT EXISTS seller (
                id SERIAL PRIMARY KEY,
                "user" INTEGER NOT NULL REFERENCES legacy_user(id),
                name VARCHAR(255) NOT NULL UNIQUE,
                logo INTEGER NOT NULL DEFAULT 0,
                location TEXT NOT NULL DEFAULT '',
                contacts JSONB NOT NULL DEFAULT '{}',
                price_list INTEGER NOT NULL DEFAULT 0,
                links TEXT NOT NULL DEFAULT '',
                about TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                status SMALLINT NOT NULL DEFAULT 0
            );

            -- review
            CREATE TABLE IF NOT EXISTS review (
                id SERIAL PRIMARY KEY,
                type SMALLINT NOT NULL DEFAULT 0,
                object INTEGER NOT NULL DEFAULT 0,
                points INTEGER NOT NULL DEFAULT 0,
                author INTEGER NOT NULL REFERENCES legacy_user(id),
                text TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                status SMALLINT NOT NULL DEFAULT 0
            );
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS review CASCADE;
            DROP TABLE IF EXISTS seller CASCADE;
            DROP TABLE IF EXISTS advert CASCADE;
            DROP TABLE IF EXISTS categories CASCADE;
            DROP TABLE IF EXISTS catalog CASCADE;
            DROP TABLE IF EXISTS legacy_user CASCADE;
            """,
        ),
    ]
