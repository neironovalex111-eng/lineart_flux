#!/bin/bash
set -e # Завершить скрипт, если любая команда вернет ошибку

echo "--- Запускается стартовый скрипт ---"

# Путь к файлу-флагу, который покажет, что модели уже скачаны
FLAG_FILE="/models_downloaded.flag"

# Проверяем, существует ли файл-флаг
if [ ! -f "$FLAG_FILE" ]; then
    echo "--- Модели не найдены, начинаю скачивание... Это может занять некоторое время. ---"

    # Создаем все нужные папки
    mkdir -p /ComfyUI/models/diffusion_models \
             /ComfyUI/models/text_encoders \
             /ComfyUI/models/vae \
             /ComfyUI/models/loras

    # Скачиваем модели, используя HF_TOKEN из переменных окружения
    wget --header="Authorization: Bearer ${HF_TOKEN}" https://huggingface.co/neishonagenc/360models/resolve/main/diffusion_models/flux1-kontext-dev.safetensors -O /ComfyUI/models/diffusion_models/flux1-kontext-dev.safetensors
    wget --header="Authorization: Bearer ${HF_TOKEN}" https://huggingface.co/neishonagenc/360models/resolve/main/text_encoders/t5xxl_fp16.safetensors -O /ComfyUI/models/text_encoders/t5xxl_fp16.safetensors
    wget --header="Authorization: Bearer ${HF_TOKEN}" https://huggingface.co/neishonagenc/360models/resolve/main/text_encoders/clip_l.safetensors -O /ComfyUI/models/text_encoders/clip_l.safetensors
    wget --header="Authorization: Bearer ${HF_TOKEN}" https://huggingface.co/neishonagenc/360models/resolve/main/vae/ae.safetensors -O /ComfyUI/models/vae/ae.safetensors
    wget https://cdn.maground.ai/eu/int/loras/MAG_14785.safetensors -O ${COMFY_PATH}/models/loras/bg.safetensors
    wget https://cdn.maground.ai/eu/int/loras/AUDI_E_TRON_GT2025_000004500.safetensors -O ${COMFY_PATH}/models/loras/car.safetensors


    echo "--- Модели успешно скачаны! ---"
    # Создаем файл-флаг, чтобы не качать снова
    touch $FLAG_FILE
else
    echo "--- Модели уже на месте, пропускаю скачивание. ---"
fi

echo "--- Запускаю ComfyUI и обработчик... ---"
# Запускаем основные процессы
python /ComfyUI/main.py & python -u /handler.py
