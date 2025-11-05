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
             /ComfyUI/models/controlnet \
             /ComfyUI/models/loras

    wget --header="Authorization: Bearer ${HF_TOKEN}" https://huggingface.co/black-forest-labs/FLUX.1-dev/resolve/main/flux1-dev.safetensors -O /ComfyUI/models/diffusion_models/flux1-dev.safetensors
    wget --header="Authorization: Bearer ${HF_TOKEN}" https://huggingface.co/neishonagenc/360models/resolve/main/text_encoders/t5xxl_fp16.safetensors -O /ComfyUI/models/text_encoders/t5xxl_fp16.safetensors
    wget --header="Authorization: Bearer ${HF_TOKEN}" https://huggingface.co/neishonagenc/360models/resolve/main/text_encoders/clip_l.safetensors -O /ComfyUI/models/text_encoders/clip_l.safetensors
    wget --header="Authorization: Bearer ${HF_TOKEN}" https://huggingface.co/neishonagenc/360models/resolve/main/vae/ae.safetensors -O /ComfyUI/models/vae/ae.safetensors
    wget --header="Authorization: Bearer ${HF_TOKEN}" https://huggingface.co/Shakker-Labs/FLUX.1-dev-ControlNet-Union-Pro/resolve/main/diffusion_pytorch_model.safetensors -O /ComfyUI/models/controlnet/union.safetensors
    wget https://cdn.maground.ai/eu/int/loras/MAG_14785.safetensors -O /ComfyUI/models/loras/bg.safetensors
    wget https://cdn.maground.ai/eu/int/loras/AUDI_E_TRON_GT2025_000004500.safetensors -O /ComfyUI/models/loras/car.safetensors


    echo "--- Модели успешно скачаны! ---"
    touch $FLAG_FILE
else
    echo "--- Модели уже на месте, пропускаю скачивание. ---"
fi

echo "--- Запускаю ComfyUI и обработчик... ---"
# Запускаем основные процессы
python /ComfyUI/main.py & python -u /handler.py
