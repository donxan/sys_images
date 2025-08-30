import os
import re
import yaml
import requests
import concurrent.futures
from distutils.version import LooseVersion
from typing import List, Dict, Any, Optional
from functools import lru_cache
import logging
import time

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 基本配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'config.yaml')
SYNC_FILE = os.path.join(BASE_DIR, 'sync.yaml')
CUSTOM_SYNC_FILE = os.path.join(BASE_DIR, 'custom_sync.yaml')

# 全局请求头
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; ImageSyncBot/1.0; +https://github.com/your-repo/image-sync)'
}

# 排除标签的关键词
EXCLUDE_KEYWORDS = ['alpha', 'beta', 'rc', 'dev', 'test', 'amd64', 'ppc64le', 'arm64', 'arm', 's390x', 'SNAPSHOT', 'debug', 'main']

# 请求超时时间（秒）
REQUEST_TIMEOUT = 30

def is_exclude_tag(tag: str) -> bool:
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

@lru_cache(maxsize=128)
def get_aliyun_tags(image_name: str) -> List[str]:
    """获取阿里云镜像仓库的所有标签"""
    try:
        # 简化版本：直接尝试访问API
        url = f"https://registry.cn-hangzhou.aliyuncs.com/v2/ctrimg/{image_name}/tags/list"
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            data = response.json()
            return data.get('tags', [])
    except Exception as e:
        logger.warning(f"获取阿里云镜像 {image_name} 标签失败: {e}")
    
    return []

def get_gcr_tags(image: str, limit: int = 5, host: str = "k8s.gcr.io") -> List[str]:
    """获取 GCR 镜像标签"""
    try:
        url = f"https://{host}/v2/{image}/tags/list"
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        tags = [tag for tag in data.get('tags', []) if not is_exclude_tag(tag)]
        tags.sort(key=LooseVersion, reverse=True)
        
        # 获取阿里云已有标签并过滤
        aliyun_tags = get_aliyun_tags(image.split('/')[-1])
        unsynced_tags = [tag for tag in tags if tag not in aliyun_tags]
        
        return unsynced_tags[:limit]
        
    except Exception as e:
        logger.error(f"获取 {host}/{image} 标签失败: {e}")
        return []

def get_quay_tags(image: str, limit: int = 5) -> List[str]:
    """获取 Quay.io 镜像标签"""
    try:
        url = f"https://quay.io/api/v1/repository/{image}/tag/?onlyActiveTags=true&limit=100"
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        tags = []
        for tag_info in data.get('tags', []):
            tag_name = tag_info.get('name', '')
            if not is_exclude_tag(tag_name):
                tags.append(tag_name)
        
        tags.sort(key=LooseVersion, reverse=True)
        
        # 获取阿里云已有标签并过滤
        aliyun_tags = get_aliyun_tags(image.split('/')[-1])
        unsynced_tags = [tag for tag in tags if tag not in aliyun_tags]
        
        return unsynced_tags[:limit]
        
    except Exception as e:
        logger.error(f"获取 quay.io/{image} 标签失败: {e}")
        return []

def get_elastic_tags(image: str, limit: int = 5) -> List[str]:
    """获取 Elastic 镜像标签"""
    try:
        # 简化版本：直接获取标签列表
        url = f"https://docker.elastic.co/v2/{image}/tags/list"
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        tags = [tag for tag in data.get('tags', []) if not is_exclude_tag(tag)]
        tags.sort(key=LooseVersion, reverse=True)
        
        # 获取阿里云已有标签并过滤
        aliyun_tags = get_aliyun_tags(image.split('/')[-1])
        unsynced_tags = [tag for tag in tags if tag not in aliyun_tags]
        
        return unsynced_tags[:limit]
        
    except Exception as e:
        logger.error(f"获取 docker.elastic.co/{image} 标签失败: {e}")
        return []

def get_ghcr_tags(image: str, limit: int = 5) -> List[str]:
    """获取 GitHub Container Registry 镜像标签"""
    try:
        # 对于公开镜像，可以直接访问
        url = f"https://ghcr.io/v2/{image}/tags/list"
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
        
        if response.status_code == 200:
            data = response.json()
            tags = [tag for tag in data.get('tags', []) if not is_exclude_tag(tag)]
            tags.sort(key=LooseVersion, reverse=True)
            
            # 获取阿里云已有标签并过滤
            aliyun_tags = get_aliyun_tags(image.split('/')[-1])
            unsynced_tags = [tag for tag in tags if tag not in aliyun_tags]
            
            return unsynced_tags[:limit]
        else:
            logger.warning(f"获取 ghcr.io/{image} 标签失败: HTTP {response.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"获取 ghcr.io/{image} 标签失败: {e}")
        return []

def get_docker_io_tags(image: str, limit: int = 5) -> List[str]:
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
            if not is_exclude_tag(tag_name):
                tags.append(tag_name)
        
        tags.sort(key=LooseVersion, reverse=True)
        
        # 获取阿里云已有标签并过滤
        aliyun_tags = get_aliyun_tags(image_name)
        unsynced_tags = [tag for tag in tags if tag not in aliyun_tags]
        
        return unsynced_tags[:limit]
        
    except Exception as e:
        logger.error(f"获取 docker.io/{image} 标签失败: {e}")
        return []

def process_single_image(repo: str, image: str, limit: int = 5) -> Dict[str, Any]:
    """处理单个镜像的标签获取"""
    repo_handlers = {
        'gcr.io': lambda img, lim: get_gcr_tags(img, lim, "gcr.io"),
        'k8s.gcr.io': lambda img, lim: get_gcr_tags(img, lim, "k8s.gcr.io"),
        'registry.k8s.io': lambda img, lim: get_gcr_tags(img, lim, "registry.k8s.io"),
        'quay.io': get_quay_tags,
        'docker.elastic.co': get_elastic_tags,
        'ghcr.io': get_ghcr_tags,
        'docker.io': get_docker_io_tags
    }
    
    try:
        if repo in repo_handlers:
            handler = repo_handlers[repo]
            sync_tags = handler(image, limit)
            return {'repo': repo, 'image': image, 'tags': sync_tags, 'success': True}
        else:
            return {'repo': repo, 'image': image, 'tags': [], 'success': False, 'error': 'Unsupported repo'}
    except Exception as e:
        logger.error(f"处理镜像 {repo}/{image} 失败: {e}")
        return {'repo': repo, 'image': image, 'tags': [], 'success': False, 'error': str(e)}

def generate_dynamic_conf() -> None:
    """生成动态同步配置（使用并发处理）"""
    logger.info("开始生成动态同步配置")
    
    try:
        with open(CONFIG_FILE, 'r') as stream:
            config = yaml.safe_load(stream)
    except Exception as e:
        logger.error(f"读取配置文件失败: {e}")
        return
    
    skopeo_sync_data = {}
    tasks = []
    
    # 准备所有任务
    for repo, images in config.get('images', {}).items():
        if not images:
            continue
            
        if repo not in skopeo_sync_data:
            skopeo_sync_data[repo] = {'images': {}}
        
        for image in images:
            tasks.append((repo, image, config.get('last', 5)))
    
    # 使用线程池并发处理
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:  # 减少并发数避免被限流
        future_to_task = {
            executor.submit(process_single_image, repo, image, limit): (repo, image) 
            for repo, image, limit in tasks
        }
        
        for future in concurrent.futures.as_completed(future_to_task):
            task_repo, task_image = future_to_task[future]
            try:
                result = future.result()
                results.append(result)
                
                if result['success'] and result['tags']:
                    skopeo_sync_data[result['repo']]['images'][result['image']] = result['tags']
                    logger.info(f"计划同步 {result['repo']}/{result['image']} 的标签: {result['tags']}")
                else:
                    logger.info(f"{result['repo']}/{result['image']} 无需同步新标签")
                    
            except Exception as e:
                logger.error(f"处理任务失败: {e}")
    
    # 保存配置
    try:
        with open(SYNC_FILE, 'w') as f:
            yaml.safe_dump(skopeo_sync_data, f, default_flow_style=False)
        logger.info(f"同步配置已保存到 {SYNC_FILE}")
        
        # 打印生成的配置摘要
        total_images = sum(len(repo_data['images']) for repo_data in skopeo_sync_data.values())
        total_tags = sum(len(tags) for repo_data in skopeo_sync_data.values() for tags in repo_data['images'].values())
        logger.info(f"生成配置摘要: {total_images} 个镜像, {total_tags} 个标签")
        
    except Exception as e:
        logger.error(f"保存同步配置失败: {e}")

def generate_custom_conf() -> None:
    """生成自定义同步配置"""
    logger.info("开始生成自定义同步配置")
    
    try:
        with open(CUSTOM_SYNC_FILE, 'r') as stream:
            custom_sync_config = yaml.safe_load(stream)
    except Exception as e:
        logger.error(f"读取自定义配置文件失败: {e}")
        return
    
    custom_skopeo_sync_data = {}
    
    for repo, repo_config in custom_sync_config.items():
        if repo not in custom_skopeo_sync_data:
            custom_skopeo_sync_data[repo] = {'images': {}}
        
        images = repo_config.get('images', {})
        if not images:
            continue
        
        for image, tags in images.items():
            if not tags:
                continue
                
            # 获取阿里云已有标签
            aliyun_tags = get_aliyun_tags(image.split('/')[-1])
            
            # 筛选未同步的标签
            unsynced_tags = [tag for tag in tags if tag not in aliyun_tags]
            
            if unsynced_tags:
                custom_skopeo_sync_data[repo]['images'][image] = unsynced_tags
                logger.info(f"计划同步自定义镜像 {repo}/{image} 的标签: {unsynced_tags}")
            else:
                logger.info(f"自定义镜像 {repo}/{image} 的所有标签已同步")
    
    try:
        with open(CUSTOM_SYNC_FILE, 'w') as f:
            yaml.safe_dump(custom_skopeo_sync_data, f, default_flow_style=False)
        logger.info(f"自定义同步配置已保存到 {CUSTOM_SYNC_FILE}")
    except Exception as e:
        logger.error(f"保存自定义同步配置失败: {e}")

if __name__ == "__main__":
    start_time = time.time()
    generate_dynamic_conf()
    generate_custom_conf()
    end_time = time.time()
    logger.info(f"同步配置生成完成，耗时: {end_time - start_time:.2f} 秒")