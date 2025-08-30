import os
import re
import yaml
import requests
import concurrent.futures
import time
from typing import List, Dict, Any, Set, Tuple
from functools import lru_cache
import logging
from urllib.parse import urlparse

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 基本配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'config.yaml')
SYNC_FILE = os.path.join(BASE_DIR, 'sync.yaml')
CUSTOM_SYNC_FILE = os.path.join(BASE_DIR, 'custom_sync.yaml')

# 目标仓库配置
TARGET_REGISTRY = "registry.cn-hangzhou.aliyuncs.com"
TARGET_NAMESPACE = "ctrimg"

# 全局请求头
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; ImageSyncBot/1.0; +https://github.com/your-repo/image-sync)'
}

# 排除标签的关键词
EXCLUDE_KEYWORDS = ['alpha', 'beta', 'rc', 'dev', 'test', 'amd64', 'ppc64le', 'arm64', 'arm', 's390x', 'SNAPSHOT', 'debug', 'main']

# 请求超时时间（秒）
REQUEST_TIMEOUT = 30
# 最大并发数
MAX_WORKERS = 10
# 每个镜像最多同步的版本数
MAX_TAGS_PER_IMAGE = 5

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
        except Exception as e:
            logger.warning(f"获取目标仓库镜像 {image_name} 标签失败: {e}")
        
        return set()

    def get_gcr_tags(self, repo: str, image: str, limit: int = MAX_TAGS_PER_IMAGE) -> List[str]:
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
                all_tags.sort(key=lambda x: tuple(map(int, x.split('.'))) if re.match(r'^\d+\.\d+\.\d+$', x) else (0, 0, 0), reverse=True)
            except:
                all_tags.sort(reverse=True)
            
            return all_tags[:limit]
            
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

    def get_quay_tags(self, repo: str, image: str, limit: int = MAX_TAGS_PER_IMAGE) -> List[str]:
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
                tags.sort(key=lambda x: tuple(map(int, x.split('.'))) if re.match(r'^\d+\.\d+\.\d+$', x) else (0, 0, 0), reverse=True)
            except:
                tags.sort(reverse=True)
            
            return tags[:limit]
            
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

    def get_elastic_tags(self, repo: str, image: str, limit: int = MAX_TAGS_PER_IMAGE) -> List[str]:
        """获取 Elastic 镜像标签"""
        try:
            url = f"https://docker.elastic.co/v2/{image}/tags/list"
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            tags = [tag for tag in data.get('tags', []) if not self.is_exclude_tag(tag)]
            
            # 按版本号排序
            try:
                tags.sort(key=lambda x: tuple(map(int, x.split('.'))) if re.match(r'^\d+\.\d+\.\d+$', x) else (0, 0, 0), reverse=True)
            except:
                tags.sort(reverse=True)
            
            return tags[:limit]
            
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

    def get_ghcr_tags(self, repo: str, image: str, limit: int = MAX_TAGS_PER_IMAGE) -> List[str]:
        """获取 GitHub Container Registry 镜像标签"""
        try:
            url = f"https://ghcr.io/v2/{image}/tags/list"
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                data = response.json()
                tags = [tag for tag in data.get('tags', []) if not self.is_exclude_tag(tag)]
                
                # 按版本号排序
                try:
                    tags.sort(key=lambda x: tuple(map(int, x.split('.'))) if re.match(r'^\d+\.\d+\.\d+$', x) else (0, 0, 0), reverse=True)
                except:
                    tags.sort(reverse=True)
                
                return tags[:limit]
            else:
                logger.warning(f"获取 {repo}/{image} 标签失败: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

    def get_docker_io_tags(self, repo: str, image: str, limit: int = MAX_TAGS_PER_IMAGE) -> List[str]:
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
                tags.sort(key=lambda x: tuple(map(int, x.split('.'))) if re.match(r'^\d+\.\d+\.\d+$', x) else (0, 0, 0), reverse=True)
            except:
                tags.sort(reverse=True)
            
            return tags[:limit]
            
        except Exception as e:
            logger.error(f"获取 {repo}/{image} 标签失败: {e}")
            return []

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

    def process_single_image(self, repo: str, image: str) -> Dict[str, Any]:
        """处理单个镜像的同步"""
        try:
            tags_to_sync = self.compare_and_generate_sync_list(repo, image)
            
            if tags_to_sync:
                logger.info(f"需要同步 {repo}/{image}: {tags_to_sync}")
                return {
                    'repo': repo,
                    'image': image,
                    'tags': tags_to_sync,
                    'success': True
                }
            else:
                logger.info(f"无需同步 {repo}/{image} (所有标签已存在或无需更新)")
                return {
                    'repo': repo,
                    'image': image,
                    'tags': [],
                    'success': True,
                    'skipped': True
                }
                
        except Exception as e:
            logger.error(f"处理镜像 {repo}/{image} 失败: {e}")
            return {
                'repo': repo,
                'image': image,
                'tags': [],
                'success': False,
                'error': str(e)
            }

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
        
        # 使用线程池并发处理
        total_images = len(tasks)
        processed_count = 0
        synced_count = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_task = {
                executor.submit(self.process_single_image, repo, image): (repo, image) 
                for repo, image in tasks
            }
            
            for future in concurrent.futures.as_completed(future_to_task):
                repo, image = future_to_task[future]
                processed_count += 1
                
                try:
                    result = future.result()
                    
                    if result['success'] and result.get('tags'):
                        sync_config[result['repo']]['images'][result['image']] = result['tags']
                        synced_count += 1
                        
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

def main():
    """主函数"""
    start_time = time.time()
    
    sync_manager = ImageSync()
    
    # 生成动态配置
    dynamic_config = sync_manager.generate_dynamic_config()
    if dynamic_config:
        sync_manager.save_config(dynamic_config, SYNC_FILE)
    
    # 生成自定义配置
    custom_config = sync_manager.generate_custom_config()
    if custom_config:
        sync_manager.save_config(custom_config, CUSTOM_SYNC_FILE)
    
    end_time = time.time()
    logger.info(f"同步配置生成完成，总耗时: {end_time - start_time:.2f} 秒")

if __name__ == "__main__":
    main()