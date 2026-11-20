"""Код субъекта РФ (двузначный, «71» — Тульская область) у региона.

``Region.code`` — текстовый ключ импорта (``tulskaya_obl``), а ``id`` —
внутренний номер записи: ни то, ни другое не является официальным кодом
субъекта. Новое поле ``subject_code`` заполняется по ключу ``code`` (а не
по ``id`` — на другой базе номера записей могут отличаться). Существующие
``code`` и ``id`` не меняются: на них завязаны импорты и ссылки отчётов.

Регион с неизвестным ключом остаётся с пустым ``subject_code``.
"""
from django.db import migrations, models

SUBJECT_CODES = {
    'adigeya_resp': '01', 'bashkortostan_resp': '02', 'buryatiya_resp': '03',
    'altay_resp': '04', 'dagestan_resp': '05', 'ingushetiya_resp': '06',
    'kabardino_balkarskaya_resp': '07', 'kalmikiya_resp': '08',
    'karachaevo_cherkesskaya_resp': '09', 'kareliya_resp': '10',
    'komi_resp': '11', 'mariiel_resp': '12', 'mordoviya_resp': '13',
    'yakutiya_saha_resp': '14', 'alaniya_severnaya_osetiya_resp': '15',
    'tatarstan_resp': '16', 'tyva_resp': '17', 'udmurtskaya_resp': '18',
    'hakasiya_resp': '19', 'chechenskaya_resp': '20',
    'chuvashskaya_resp': '21', 'altayskiy_kray': '22',
    'krasnodarskiy_kray': '23', 'krasnoyarskiy_kray': '24',
    'primorskiy_kray': '25', 'stavrapolskiy_kray': '26',
    'habarjvskiy_kray': '27', 'amurskaya_obl': '28',
    'arhangelskaya_obl': '29', 'astrahanskaya_obl': '30',
    'belgorodskaya_obl': '31', 'bryanskaya_obl': '32',
    'vladimirskaya_obl': '33', 'volgogradskaya_obl': '34',
    'vologodskaya_obl': '35', 'voronezhskaya_obl': '36',
    'ivanovskaya_obl': '37', 'irkutskaya_obl': '38',
    'kaliningradskaya_obl': '39', 'kaluzhskaya_obl': '40',
    'kamchatskiy_kray': '41', 'kemerovskaya_obl': '42',
    'kirovskaya_obl': '43', 'kostromskaya_obl': '44',
    'kurganskaya_obl': '45', 'kurskaya_obl': '46',
    'leningradskaya_obl': '47', 'lipetskaya_obl': '48',
    'magadanskaya_obl': '49', 'moskovskaya_obl': '50',
    'murmanskaya_obl': '51', 'nizhegorodskaya_obl': '52',
    'novgorodskaya_obl': '53', 'novosibirskaya_obl': '54',
    'omskaya_obl': '55', 'orenburgskaya_obl': '56', 'orlovskaya_obl': '57',
    'penzenskaya_obl': '58', 'permskiy_kray': '59', 'pskovskaya_obl': '60',
    'rostovskaya_obl': '61', 'ryazanskaya_obl': '62',
    'samarskaya_obl': '63', 'saratovskaya_obl': '64',
    'sakhalinskaya_obl': '65', 'sverdlovskaya_obl': '66',
    'smolenskaya_obl': '67', 'tambovskaya_obl': '68', 'tverskaya_obl': '69',
    'tomskaya_obl': '70', 'tulskaya_obl': '71', 'tumenskaya_obl': '72',
    'ulyanovskaya_obl': '73', 'chelyabinskaya_obl': '74',
    'zabaikalskiy_kray': '75', 'yaroslavskaya_obl': '76', 'msk': '77',
    'spb': '78', 'evreyskaya_ao': '79', 'nenetskiy_ao': '83',
    'hanty_mansiyskiy_ao': '86', 'chukotskiy_ao': '87',
    'yamalo_nenetskiy_ao': '89', 'krim_resp': '91', 'sev_city': '92',
}


def fill_subject_codes(apps, schema_editor):
    Region = apps.get_model('agrocosmos', 'Region')
    for key, subject_code in SUBJECT_CODES.items():
        Region.objects.filter(code=key).update(subject_code=subject_code)


class Migration(migrations.Migration):

    dependencies = [
        ('agrocosmos', '0045_crop_season_phenology_fields'),
    ]

    operations = [
        migrations.AlterField(
            model_name='region',
            name='code',
            field=models.CharField(
                max_length=100, unique=True, verbose_name='Ключ региона',
            ),
        ),
        migrations.AddField(
            model_name='region',
            name='subject_code',
            field=models.CharField(
                blank=True, null=True, max_length=2, unique=True,
                verbose_name='Код субъекта РФ',
                help_text='Двузначный код субъекта РФ: 71 — Тульская область.',
            ),
        ),
        migrations.RunPython(fill_subject_codes, migrations.RunPython.noop),
    ]
