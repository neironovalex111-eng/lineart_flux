import base64
from io import BytesIO
import json
import os
import time
import traceback
import uuid

import requests
import runpod
import websocket

# --- НАСТРОЙКИ ---
# Адрес API ComfyUI, оставляем как есть
COMFY_HOST = "127.0.0.1:8188"

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

def get_final_image_as_base64(prompt_id, output_node_id):
    """Ждет результат, скачивает картинку и возвращает ее в Base64."""
    try:
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

        print(f"Забираем финальный файл: {filename}")
        image_url = f"http://{COMFY_HOST}/view?filename={filename}&subfolder={subfolder}&type={img_type}"
        image_response = requests.get(image_url, timeout=60)
        image_response.raise_for_status()

        return base64.b64encode(image_response.content).decode('utf-8')

    except Exception as e:
        print(f"Пиздец, не удалось получить результат: {e}")
        raise

def find_node_ids(workflow):
    """
    Гениальная и простая функция.
    Находит ID нод для загрузки и сохранения картинки по их типу.
    """
    load_image_node_id = None
    save_image_node_id = None
    
    # Ищем ноды, которые могут принимать входное изображение. 'LoadImageOutput' - это из твоего wf.
    possible_input_nodes = ['LoadImage', 'LoadImageOutput']

    for node_id, node_data in workflow.items():
        if node_data.get('class_type') in possible_input_nodes:
            load_image_node_id = node_id
        elif node_data.get('class_type') == 'SaveImage':
            save_image_node_id = node_id
            
    if not all([load_image_node_id, save_image_node_id]):
        raise ValueError(f"Ебать, не смог найти ноды. Нашел Load: {load_image_node_id}, Save: {save_image_node_id}. Проверь воркфлоу.")
        
    print(f"Ноды найдены. Вход: {load_image_node_id}, Выход: {save_image_node_id}")
    return load_image_node_id, save_image_node_id


def handler(job):
    job_input = job.get('input', {})

    base64_image = job_input.get("image")
    prompt_workflow = job_input.get("workflow") # <--- Получаем воркфлоу из инпута

    if not base64_image:
        return {"error": "Бро, ты забыл передать 'image' в base64."}
    if not prompt_workflow:
        return {"error": "Бро, а где 'workflow' в формате JSON?"}

    client_id = str(uuid.uuid4())
    ws = None
    
    try:
        # 1. Находим нужные ID нод в присланном воркфлоу
        load_image_node_id, save_image_node_id = find_node_ids(prompt_workflow)

        # 2. Загружаем картинку
        uploaded_image_info = upload_image(base64_image)
        uploaded_filename = uploaded_image_info['name']
        
        # 3. Подставляем имя файла в воркфлоу
        prompt_workflow[load_image_node_id]['inputs']['image'] = uploaded_filename

        # 4. Отправляем задачу в очередь
        queued_data = queue_prompt(prompt_workflow, client_id)
        prompt_id = queued_data['prompt_id']
        print(f"Задача поставлена в очередь с ID: {prompt_id}")

        # 5. Слушаем WebSocket до завершения
        ws_url = f"ws://{COMFY_HOST}/ws?clientId={client_id}"
        ws = websocket.create_connection(ws_url, timeout=10)
        
        while True:
            out = ws.recv()
            if isinstance(out, str):
                message = json.loads(out)
                if message.get('type') == 'executing' and message.get('data', {}).get('node') is None and message['data']['prompt_id'] == prompt_id:
                    print("Выполнение задачи завершено.")
                    break
        ws.close()

        # 6. Получаем результат
        final_image_base64 = get_final_image_as_base64(prompt_id, save_image_node_id)
        
        return {"image_base64": final_image_base64}
        
    except Exception as e:
        traceback.print_exc()
        return {"error": f"Произошла глобальная ошибка: {str(e)}"}
    finally:
        if ws and ws.connected:
            ws.close()

if __name__ == "__main__":
    if check_server_ready(f"http://{COMFY_HOST}/"):
        runpod.serverless.start({"handler": handler})
