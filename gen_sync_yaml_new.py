import os
import re
import yaml
import requests
import concurrent.futures
import time
import json
import subprocess
import threading
from typing import List, Dict, Any, Set, Tuple
from functools import lru_cache
import logging
from queue import Queue

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 基本配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'config.yaml')
SYNC_FILE = os.path.join(BASE_DIR, 'sync.yaml')
CUSTOM_SYNC_FILE = os.path.join(BASE_DIR, 'custom_sync.yaml')
SYNC_LOG_FILE = os.path.join(BASE_DIR, 'sync.log')

# 目标仓库配置
TARGET_REGISTRY = "registry.cn-hangzhou.aliyuncs.com"
TARGET_NAMESPACE = "ctrimg"
TARGET_REPO = f"{TARGET_REGISTRY}/{TARGET_NAMESPACE}"

# 同步配置
MAX_WORKERS = 8  # 并发同步的镜像数量
MAX_TAGS_PER_IMAGE = 5  # 每个镜像最多同步的版本数
SYNC_TIMEOUT = 1800  # 单个镜像同步超时时间（秒）

# 全局请求头
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; ImageSyncBot/1.0; +https://github.com/your-repo/image-sync)'
}

# 排除标签的关键词
EXCLUDE_KEYWORDS = ['alpha', 'beta', 'rc', 'dev', 'test', 'amd64', 'ppc64le', 'arm64', 'arm', 's390x', 'SNAPSHOT', 'debug', 'main']

# 请求超时时间（秒）
REQUEST_TIMEOUT = 30

class ImageSync:
    def __init__(self):
        self.source_handlers = {
            'gcr.io': self.get_gcr_tags,
            'k8s.gcr.io': self.get_gcr_tags,
            'registry.k8s.io': self.get_gcr_tags,
            'quay.io': self.get_quay_tags,
            'docker.elastic.co': self.get_elastic_tags,
            'ghcr.io': self.get_ghcr_tags,
            'docker.io': self.get_docker_io_tags
        }
        self.sync_queue = Queue()
        self.sync_results = {}
        self.lock = threading.Lock()
    
    def is_exclude_tag(self, tag: str) -> bool:
        """判断是否是需要排除的标签"""
        if not tag or not isinstance(tag, str):
            return True
            
        tag_lower = tag.lower()
        
        # 排除过长的标签（可能是哈希值）
        if len(tag) >= 40:
            return True
            
        # 排除包含关键词的标签
        for keyword in EXCLUDE_KEYWORDS:
            if keyword.lower() in tag_lower:
                return True
                
        # 处理特定模式的标签
        if re.search(r"-\w{9,}", tag):  # 类似提交哈希的模式
            return True
            
        # 允许带有数字后缀的标签（如v1.2.3-1）
        if re.search(r"-\d+$", tag):
            return False
            
        # 排除其他带有连字符的标签
        if '-' in tag:
            return True
            
        return False

    @lru_cache(maxsize=1024)
    def get_target_tags(self, image_name: str) -> Set[str]:
        """获取目标仓库的镜像标签"""
        try:
            url = f"https://{TARGET_REGISTRY}/v2/{TARGET_NAMESPACE}/{image_name}/tags/list"
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                return set(data.get('tags', []))
            else:
                logger.warning(f"获取目标仓库镜像 {image_name} 标签失败: HTTP {response.status_code}")
        except Exception as e:
            logger.warning(f"获取目标仓库镜像 {image_name} 标签失败: {e}")
        
        return set()

    def get_gcr_tags(self, repo: str, image: str) -> List[str]:
        """获取 GCR 镜像标签"""
        try:
            host = repo
            url = f"https://{host}/v2/{image}/tags/list"
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            # 获取所有标签并过滤
            all_tags = [tag for tag in data.get('tags', []) if not self.is_exclude_tag(tag)]
            
            # 按版本号排序（从新到旧）
            try:
                all_tags.sort(key=self._version_key, reverse=True)
            except:
                all_tags.sort(reverse=True)
            
            return all_tags
            
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

    def get_quay_tags(self, repo: str, image: str) -> List[str]:
        """获取 Quay.io 镜像标签"""
        try:
            url = f"https://quay.io/api/v1/repository/{image}/tag/?onlyActiveTags=true&limit=100"
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            tags = []
            for tag_info in data.get('tags', []):
                tag_name = tag_info.get('name', '')
                if not self.is_exclude_tag(tag_name):
                    tags.append(tag_name)
            
            # 按版本号排序
            try:
                tags.sort(key=self._version_key, reverse=True)
            except:
                tags.sort(reverse=True)
            
            return tags
            
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

    def get_elastic_tags(self, repo: str, image: str) -> List[str]:
        """获取 Elastic 镜像标签"""
        try:
            url = f"https://docker.elastic.co/v2/{image}/tags/list"
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            tags = [tag for tag in data.get('tags', []) if not self.is_exclude_tag(tag)]
            
            # 按版本号排序
            try:
                tags.sort(key=self._version_key, reverse=True)
            except:
                tags.sort(reverse=True)
            
            return tags
            
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

    def get_ghcr_tags(self, repo: str, image: str) -> List[str]:
        """获取 GitHub Container Registry 镜像标签"""
        try:
            url = f"https://ghcr.io/v2/{image}/tags/list"
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                data = response.json()
                tags = [tag for tag in data.get('tags', []) if not self.is_exclude_tag(tag)]
                
                # 按版本号排序
                try:
                    tags.sort(key=self._version_key, reverse=True)
                except:
                    tags.sort(reverse=True)
                
                return tags
            else:
                logger.warning(f"获取 {repo}/{image} 标签失败: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

    def get_docker_io_tags(self, repo: str, image: str) -> List[str]:
        """获取 Docker Hub 镜像标签"""
        try:
            namespace_image = image.split('/')
            if len(namespace_image) != 2:
                logger.error(f"Docker Hub 镜像名称格式错误: {image}")
                return []
            
            username, image_name = namespace_image
            url = f"https://hub.docker.com/v2/namespaces/{username}/repositories/{image_name}/tags?page=1&page_size=100"
            
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            tags = []
            for tag_info in data.get('results', []):
                tag_name = tag_info.get('name', '')
                if not self.is_exclude_tag(tag_name):
                    tags.append(tag_name)
            
            # 按版本号排序
            try:
                tags.sort(key=self._version_key, reverse=True)
            except:
                tags.sort(reverse=True)
            
            return tags
            
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

    def _version_key(self, version: str) -> tuple:
        """版本号排序键"""
        try:
            # 处理 v1.2.3 格式
            if version.startswith('v'):
                version = version[1:]
            
            # 分割版本号
            parts = []
            for part in version.split('.'):
                if part.isdigit():
                    parts.append(int(part))
                else:
                    parts.append(part)
            return tuple(parts)
        except:
            return (0, 0, 0)

    def get_source_tags(self, repo: str, image: str) -> List[str]:
        """获取源仓库的镜像标签"""
        if repo in self.source_handlers:
            return self.source_handlers[repo](repo, image)
        return []

    def compare_and_generate_sync_list(self, repo: str, image: str) -> List[str]:
        """比较源和目标标签，生成需要同步的标签列表"""
        try:
            # 获取源标签
            source_tags = self.get_source_tags(repo, image)
            if not source_tags:
                return []
            
            # 获取目标标签
            target_image_name = image.split('/')[-1]  # 提取基础镜像名
            target_tags = self.get_target_tags(target_image_name)
            
            # 找出需要同步的标签（在源中存在但在目标中不存在）
            tags_to_sync = [tag for tag in source_tags if tag not in target_tags]
            
            # 限制每个镜像最多同步的版本数
            return tags_to_sync[:MAX_TAGS_PER_IMAGE]
            
        except Exception as e:
            logger.error(f"比较镜像 {repo}/{image} 标签失败: {e}")
            return []

    def sync_single_image(self, repo: str, image: str, tags: List[str]) -> Dict[str, Any]:
        """同步单个镜像的多个标签"""
        if not tags:
            return {'success': True, 'skipped': True}
        
        target_image_name = image.split('/')[-1]
        target_image = f"{TARGET_REPO}/{target_image_name}"
        source_image = f"{repo}/{image}"
        
        results = []
        for tag in tags:
            try:
                # 使用 skopeo copy 命令同步单个标签
                cmd = [
                    'skopeo', 'copy',
                    '--src-tls-verify=false',
                    '--dest-tls-verify=false',
                    f'docker://{source_image}:{tag}',
                    f'docker://{target_image}:{tag}'
                ]
                
                logger.info(f"开始同步: {source_image}:{tag} -> {target_image}:{tag}")
                start_time = time.time()
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=SYNC_TIMEOUT
                )
                
                duration = time.time() - start_time
                
                if result.returncode == 0:
                    logger.info(f"✅ 同步成功: {source_image}:{tag} (耗时: {duration:.1f}s)")
                    results.append({
                        'tag': tag,
                        'success': True,
                        'duration': duration
                    })
                else:
                    logger.error(f"❌ 同步失败: {source_image}:{tag}, 错误: {result.stderr}")
                    results.append({
                        'tag': tag,
                        'success': False,
                        'error': result.stderr,
                        'duration': duration
                    })
                    
            except subprocess.TimeoutExpired:
                logger.error(f"⏰ 同步超时: {source_image}:{tag}")
                results.append({
                    'tag': tag,
                    'success': False,
                    'error': 'timeout',
                    'duration': SYNC_TIMEOUT
                })
            except Exception as e:
                logger.error(f"❌ 同步异常: {source_image}:{tag}, 异常: {e}")
                results.append({
                    'tag': tag,
                    'success': False,
                    'error': str(e)
                })
        
        # 统计结果
        success_count = sum(1 for r in results if r['success'])
        total_count = len(results)
        
        return {
            'source': source_image,
            'target': target_image,
            'results': results,
            'success_count': success_count,
            'total_count': total_count,
            'success': success_count == total_count
        }

    def sync_worker(self):
        """同步工作线程"""
        while True:
            try:
                task = self.sync_queue.get()
                if task is None:  # 结束信号
                    break
                
                repo, image, tags = task
                result = self.sync_single_image(repo, image, tags)
                
                with self.lock:
                    key = f"{repo}/{image}"
                    self.sync_results[key] = result
                
                self.sync_queue.task_done()
                
            except Exception as e:
                logger.error(f"同步工作线程异常: {e}")

    def concurrent_sync_images(self, sync_config: Dict[str, Any]) -> Dict[str, Any]:
        """并发同步所有镜像"""
        # 准备同步任务
        tasks = []
        for repo, repo_config in sync_config.items():
            for image, tags in repo_config.get('images', {}).items():
                if tags:
                    tasks.append((repo, image, tags))
        
        if not tasks:
            logger.info("没有需要同步的镜像")
            return {}
        
        logger.info(f"开始并发同步 {len(tasks)} 个镜像")
        
        # 启动工作线程
        threads = []
        for _ in range(min(MAX_WORKERS, len(tasks))):
            thread = threading.Thread(target=self.sync_worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # 添加任务到队列
        for task in tasks:
            self.sync_queue.put(task)
        
        # 等待所有任务完成
        self.sync_queue.join()
        
        # 发送结束信号
        for _ in range(len(threads)):
            self.sync_queue.put(None)
        
        # 等待所有线程结束
        for thread in threads:
            thread.join(timeout=10)
        
        return self.sync_results

    def generate_dynamic_config(self) -> Dict[str, Any]:
        """生成动态同步配置"""
        logger.info("开始生成动态同步配置")
        
        try:
            with open(CONFIG_FILE, 'r') as stream:
                config = yaml.safe_load(stream)
        except Exception as e:
            logger.error(f"读取配置文件失败: {e}")
            return {}
        
        sync_config = {}
        tasks = []
        
        # 准备所有任务
        for repo, images in config.get('images', {}).items():
            if not images:
                continue
                
            if repo not in sync_config:
                sync_config[repo] = {'images': {}}
            
            for image in images:
                tasks.append((repo, image))
        
        # 使用线程池并发处理标签获取
        total_images = len(tasks)
        processed_count = 0
        synced_count = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_task = {
                executor.submit(self.compare_and_generate_sync_list, repo, image): (repo, image) 
                for repo, image in tasks
            }
            
            for future in concurrent.futures.as_completed(future_to_task):
                repo, image = future_to_task[future]
                processed_count += 1
                
                try:
                    tags_to_sync = future.result()
                    
                    if tags_to_sync:
                        sync_config[repo]['images'][image] = tags_to_sync
                        synced_count += 1
                        logger.info(f"需要同步 {repo}/{image}: {tags_to_sync}")
                    else:
                        logger.info(f"无需同步 {repo}/{image}")
                        
                    # 打印进度
                    if processed_count % 5 == 0:
                        logger.info(f"处理进度: {processed_count}/{total_images}")
                        
                except Exception as e:
                    logger.error(f"处理任务失败: {e}")
        
        # 统计信息
        total_tags = sum(len(tags) for repo_data in sync_config.values() for tags in repo_data['images'].values())
        logger.info(f"同步配置生成完成: {synced_count}/{total_images} 个镜像需要同步, 共 {total_tags} 个标签")
        
        return sync_config

    def generate_custom_config(self) -> Dict[str, Any]:
        """生成自定义同步配置"""
        logger.info("开始生成自定义同步配置")
        
        try:
            with open(CUSTOM_SYNC_FILE, 'r') as stream:
                custom_config = yaml.safe_load(stream)
        except Exception as e:
            logger.error(f"读取自定义配置文件失败: {e}")
            return {}
        
        sync_config = {}
        
        for repo, repo_config in custom_config.items():
            if repo not in sync_config:
                sync_config[repo] = {'images': {}}
            
            images = repo_config.get('images', {})
            if not images:
                continue
            
            for image, expected_tags in images.items():
                if not expected_tags:
                    continue
                    
                # 获取目标标签
                target_image_name = image.split('/')[-1]
                target_tags = self.get_target_tags(target_image_name)
                
                # 筛选需要同步的标签
                tags_to_sync = [tag for tag in expected_tags if tag not in target_tags]
                
                if tags_to_sync:
                    sync_config[repo]['images'][image] = tags_to_sync
                    logger.info(f"需要同步自定义镜像 {repo}/{image}: {tags_to_sync}")
        
        return sync_config

    def save_config(self, config: Dict[str, Any], file_path: str) -> bool:
        """保存配置到文件"""
        try:
            with open(file_path, 'w') as f:
                yaml.safe_dump(config, f, default_flow_style=False)
            logger.info(f"配置已保存到 {file_path}")
            return True
        except Exception as e:
            logger.error(f"保存配置到 {file_path} 失败: {e}")
            return False

    def save_sync_results(self, results: Dict[str, Any]):
        """保存同步结果到文件"""
        try:
            with open(SYNC_LOG_FILE, 'w') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"同步结果已保存到 {SYNC_LOG_FILE}")
        except Exception as e:
            logger.error(f"保存同步结果失败: {e}")

def main():
    """主函数"""
    start_time = time.time()
    
    sync_manager = ImageSync()
    
    # 生成动态配置
    logger.info("=== 生成动态同步配置 ===")
    dynamic_config = sync_manager.generate_dynamic_config()
    if dynamic_config:
        sync_manager.save_config(dynamic_config, SYNC_FILE)
    
    # 生成自定义配置
    logger.info("=== 生成自定义同步配置 ===")
    custom_config = sync_manager.generate_custom_config()
    if custom_config:
        sync_manager.save_config(custom_config, CUSTOM_SYNC_FILE)
    
    # 合并所有需要同步的配置
    all_sync_config = {}
    if dynamic_config:
        for repo, repo_config in dynamic_config.items():
            if repo not in all_sync_config:
                all_sync_config[repo] = {'images': {}}
            all_sync_config[repo]['images'].update(repo_config['images'])
    
    if custom_config:
        for repo, repo_config in custom_config.items():
            if repo not in all_sync_config:
                all_sync_config[repo] = {'images': {}}
            all_sync_config[repo]['images'].update(repo_config['images'])
    
    # 执行同步
    if all_sync_config:
        logger.info("=== 开始并发同步镜像 ===")
        sync_results = sync_manager.concurrent_sync_images(all_sync_config)
        sync_manager.save_sync_results(sync_results)
        
        # 统计同步结果
        total_sync = sum(len(repo_config['images']) for repo_config in all_sync_config.values())
        success_sync = sum(1 for result in sync_results.values() if result.get('success', False))
        
        logger.info(f"同步完成: {success_sync}/{total_sync} 个镜像同步成功")
    else:
        logger.info("没有需要同步的镜像")
    
    end_time = time.time()
    logger.info(f"整个流程完成，总耗时: {end_time - start_time:.2f} 秒")

if __name__ == "__main__":
    main()