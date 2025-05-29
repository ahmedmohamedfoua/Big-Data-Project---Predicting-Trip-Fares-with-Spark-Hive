import pandas as pd
import math
import zipfile
from pathlib import Path
import os


def process_dataframe(df, output_path, target_rows=1_000_000):
    # Список всех временных меток из схемы
    timestamp_columns = [
        'request_datetime',
        'on_scene_datetime',
        'pickup_datetime',
        'dropoff_datetime'
    ]

    # Конвертация всех временных меток
    for col in timestamp_columns:
        if col in df.columns:
            if df[col].dtype == 'int64':
                df[col] = pd.to_datetime(df[col], unit='us', origin='unix')
            # Удаление строк с пропущенными временными метками
            df = df.dropna(subset=[col])

    # Извлечение дня из pickup_datetime для группировки
    df['day'] = df['pickup_datetime'].dt.date

    # Группировка по дням
    daily_counts = df.groupby('day').size().reset_index(name='count')
    total_original = daily_counts['count'].sum()

    if total_original <= target_rows:
        df.drop(columns=['day']).to_parquet(output_path, index=False)
        return

    # Оптимизация памяти для строковых колонок
    str_cols = [
        'hvfhs_license_num',
        'dispatching_base_num',
        'originating_base_num',
        'shared_request_flag',
        'shared_match_flag',
        'access_a_ride_flag',
        'wav_request_flag',
        'wav_match_flag'
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # Распределение строк с гарантией минимума
    num_days = len(daily_counts)
    remaining = target_rows - num_days

    # Пропорциональное распределение
    daily_counts['weight'] = daily_counts['count'] / daily_counts['count'].sum()
    daily_counts['allocated'] = (daily_counts['weight'] * remaining).round().astype(int)

    # Корректировка распределения
    total_allocated = daily_counts['allocated'].sum()
    if total_allocated != remaining:
        adjust = remaining - total_allocated
        daily_counts = daily_counts.sort_values('weight', ascending=False)
        daily_counts.iloc[:abs(adjust), -1] += 1 if adjust > 0 else -1

    # Сборка финального датафрейма
    daily_counts['sample_size'] = daily_counts['allocated'] + 1
    sampled_dfs = []

    for day_info in daily_counts.itertuples():
        day_data = df[df['day'] == day_info.day]
        sampled = day_data.sample(
            n=min(day_info.sample_size, len(day_data)),
            random_state=42
        )
        sampled_dfs.append(sampled)

    final_df = pd.concat(sampled_dfs)
    final_df = final_df.drop(columns=['day'])

    # Фильтрация до точного количества строк
    if len(final_df) > target_rows:
        final_df = final_df.sample(n=target_rows, random_state=42)

    # Восстановление исходных типов
    for col in str_cols:
        if col in final_df.columns:
            final_df[col] = final_df[col].astype(str)

    final_df.to_parquet(output_path, index=False)
    print(f"Processed: {output_path.name} | Rows: {len(final_df)}")


# Обработка архива
output_dir = Path('processed_data')
output_dir.mkdir(exist_ok=True)

# Правильный путь к архиву (относительный путь)
zip_path = Path('data.zip')

if not zip_path.exists():
    raise FileNotFoundError(f"Архив {zip_path} не найден в корне проекта!")

with zipfile.ZipFile(zip_path) as archive:
    parquet_files = [f for f in archive.namelist() if f.endswith('.parquet')]

    for file in parquet_files:
        output_path = output_dir / os.path.basename(file)
        with archive.open(file) as pf:
            df = pd.read_parquet(pf)
            process_dataframe(df, output_path)

print("Все файлы успешно обработаны!")