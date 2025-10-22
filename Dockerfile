FROM nvidia/cuda:12.8.0-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

# Устанавливаем базовые пакеты
RUN apt-get update && apt-get install -y --no-install-recommends git wget libgl1-mesa-glx libglib2.0-0 python3.11 python3.11-venv && rm -rf /var/lib/apt/lists/*

# Устанавливаем ComfyUI
WORKDIR /
RUN git clone https://github.com/comfyanonymous/ComfyUI.git
COPY ld.json .
COPY handler.py .
COPY start.sh .
COPY requirements.txt .

# Настраиваем venv
RUN python3.11 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"


# Устанавливаем зависимости Python
WORKDIR /ComfyUI
RUN python -m pip install --upgrade pip && python -m pip install uv && \
    uv pip install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cu128 --no-cache-dir && \
    uv pip install --upgrade -r /requirements.txt --no-cache-dir

# Устанавливаем кастомные ноды
WORKDIR /ComfyUI/custom_nodes
RUN git clone https://github.com/ltdrdata/ComfyUI-Manager comfyui-manager && \
    git clone https://github.com/pythongosssss/ComfyUI-Custom-Scripts && \
    git clone https://github.com/Fannovel16/comfyui_controlnet_aux && \
    git clone https://github.com/rgthree/rgthree-comfy && \
    git clone https://github.com/kijai/ComfyUI-KJNodes && \
    git clone https://github.com/kijai/ComfyUI-Florence2 && \
    git clone https://github.com/ssitu/ComfyUI_UltimateSDUpscale && \
    git clone https://github.com/Suzie1/ComfyUI_Comfyroll_CustomNodes && \
    git clone https://github.com/welltop-cn/ComfyUI-TeaCache && \
    git clone https://github.com/cubiq/ComfyUI_essentials && \
    git clone http://github.com/Jonseed/ComfyUI-Detail-Daemon && \
    git clone https://github.com/Acly/comfyui-tooling-nodes && \
    git clone https://github.com/BadCafeCode/masquerade-nodes-comfyui && \
    git clone https://github.com/filliptm/ComfyUI_Fill-Nodes && \
    git clone https://github.com/bash-j/mikey_nodes && \
    git clone https://github.com/evanspearman/ComfyMath && \
    git clone https://github.com/alexopus/ComfyUI-Image-Saver && \
    git clone https://github.com/ProGamerGov/ComfyUI_preview360panorama

# Устанавливаем зависимости кастомных нод
RUN for d in */ ; do \
        if [ "$d" != "comfyui-manager/" ] && [ -f "${d}requirements.txt" ]; then \
            echo "--- Installing requirements for $d ---"; \
            pip install -r "${d}requirements.txt"; \
        fi; \
    done

WORKDIR /

# Делаем наш скрит исполняемым
RUN chmod +x /start.sh

# Запускаем наш скрипт как точку входа
CMD ./start.sh
