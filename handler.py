import base64
from io import BytesIO
import json
import os
import time
import uuid

import requests
import runpod
import websocket

# --- НАСТРОЙКИ ---
# Адрес API ComfyUI
COMFY_HOST = "127.0.0.1:8188"
# ID ноды, куда грузить картинку (твоя LoadImage нода)
LOAD_IMAGE_NODE_ID = '142'  # <--- ЗАМЕНИ НА СВОЙ ID
# ID ноды, откуда забирать результат (твоя SaveImage или Preview нода)
SAVE_IMAGE_NODE_ID = '136'  # <--- ЗАМЕНИ НА СВОЙ ID
# Имя файла с workflow в API-формате
WORKFLOW_FILE = '360.json' # <--- УБЕДИСЬ, ЧТО ИМЯ ПРАВИЛЬНОЕ


def check_server_ready(url, retries=500, delay=50):
    """Ждет, пока сервер ComfyUI не станет доступен."""
    print(f"Ожидаем готовности ComfyUI по адресу {url}...")
    for i in range(retries):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print("ComfyUI готов к работе!")
                return True
        except requests.RequestException:
            pass
        time.sleep(delay / 1000)
    print(f"Пиздец, ComfyUI не поднялся после {retries} попыток.")
    return False

def upload_image(base64_string, filename="input_image.png"):
    """Загружает картинку из Base64 в папку input ComfyUI."""
    try:
        if ',' in base64_string:
            base64_data = base64_string.split(',', 1)[1]
        else:
            base64_data = base64_string
        
        image_bytes = base64.b64decode(base64_data)
        
        files = {
            'image': (filename, BytesIO(image_bytes), 'image/png'),
            'overwrite': (None, 'true'),
            'type': (None, 'input')
        }
        
        response = requests.post(f"http://{COMFY_HOST}/upload/image", files=files, timeout=30)
        response.raise_for_status()
        print(f"Картинка '{filename}' успешно загружена.")
        return response.json()
    except Exception as e:
        print(f"Пиздец, не удалось загрузить картинку: {e}")
        raise

def queue_prompt(prompt_workflow, client_id):
    """Отправляет workflow в очередь и возвращает prompt_id."""
    payload = {"prompt": prompt_workflow, "client_id": client_id}
    data = json.dumps(payload).encode('utf-8')
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(f"http://{COMFY_HOST}/prompt", data=data, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Пиздец, не удалось поставить в очередь: {e}")
        raise

def get_final_image_url(prompt_id, output_node_id):
    """Ждет результат и вытаскивает URL готовой картинки."""
    try:
        response = requests.get(f"http://{COMFY_HOST}/history/{prompt_id}", timeout=60)
        response.raise_for_status()
        history = response.json()

        if prompt_id not in history:
            raise RuntimeError("ID задачи не найден в истории. Возможно, произошла ошибка.")

        prompt_output = history[prompt_id]['outputs'].get(output_node_id)
        if not prompt_output or 'images' not in prompt_output:
            raise RuntimeError(f"В ноде {output_node_id} не найдено изображений.")
        
        image_data = prompt_output['images'][0]
        filename = image_data['filename']
        subfolder = image_data['subfolder']
        img_type = image_data['type']

        return f"http://{COMFY_HOST}/view?filename={filename}&subfolder={subfolder}&type={img_type}"
    except Exception as e:
        print(f"Пиздец, не удалось получить результат: {e}")
        raise

def get_final_image_as_base64(prompt_id, output_node_id):
    """Ждет результат, скачивает картинку и возвращает ее в Base64."""
    try:
        # Сначала получаем историю, чтобы найти имя файла
        history_response = requests.get(f"http://{COMFY_HOST}/history/{prompt_id}", timeout=60)
        history_response.raise_for_status()
        history = history_response.json()

        if prompt_id not in history:
            raise RuntimeError("ID задачи не найден в истории.")

        prompt_output = history[prompt_id]['outputs'].get(output_node_id)
        if not prompt_output or 'images' not in prompt_output:
            raise RuntimeError(f"В ноде {output_node_id} не найдено изображений.")
        
        image_data = prompt_output['images'][0]
        filename = image_data['filename']
        subfolder = image_data['subfolder']
        img_type = image_data['type']

        # Теперь, зная имя файла, "сходим" за ним как курьер
        print(f"Забираем финальный файл: {filename}")
        image_url = f"http://{COMFY_HOST}/view?filename={filename}&subfolder={subfolder}&type={img_type}"
        image_response = requests.get(image_url, timeout=60)
        image_response.raise_for_status()

        # Кодируем бинарные данные в Base64 и возвращаем
        return base64.b64encode(image_response.content).decode('utf-8')

    except Exception as e:
        print(f"Пиздец, не удалось получить результат: {e}")
        raise

def handler(job):
    job_input = job.get('input', {})

    base64_image = job_input.get("image")
    if not base64_image:
        return {"error": "Бро, ты забыл передать 'image_base64' в запросе."}

    client_id = str(uuid.uuid4())
    ws = None
    
    try:
        # 1. Загружаем картинку
        uploaded_image_info = upload_image(base64_image)
        uploaded_filename = uploaded_image_info['name']

        # 2. Загружаем и модифицируем workflow
        with open(WORKFLOW_FILE, 'r') as f:
            prompt_workflow = json.load(f)
        
        prompt_workflow[LOAD_IMAGE_NODE_ID]['inputs']['image'] = uploaded_filename

        # 3. Отправляем задачу в очередь
        queued_data = queue_prompt(prompt_workflow, client_id)
        prompt_id = queued_data['prompt_id']
        print(f"Задача поставлена в очередь с ID: {prompt_id}")

        # 4. Слушаем WebSocket до завершения
        ws_url = f"ws://{COMFY_HOST}/ws?clientId={client_id}"
        ws = websocket.create_connection(ws_url, timeout=10)
        
        execution_done = False
        while not execution_done:
            out = ws.recv()
            if isinstance(out, str):
                message = json.loads(out)
                if message.get('type') == 'executing' and message.get('data', {}).get('node') is None:
                    if message['data']['prompt_id'] == prompt_id:
                        print("Выполнение задачи завершено.")
                        execution_done = True
                        break
        
        ws.close()

        # 5. Получаем результат
        final_image_base64 = get_final_image_as_base64(prompt_id, SAVE_IMAGE_NODE_ID)
        
        return {"image_base64": final_image_base64}
        
    except Exception as e:
        # Эта строчка важна для отладки, она покажет полную ошибку
        import traceback
        traceback.print_exc()
        return {"error": f"Произошла глобальная ошибка: {e}"}
    finally:
        if ws and ws.connected:
            ws.close()


if __name__ == "__main__":
    if check_server_ready(f"http://{COMFY_HOST}/"):
        runpod.serverless.start({"handler": handler})
