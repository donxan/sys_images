import os
import re
import yaml
import requests
from distutils.version import LooseVersion
from datetime import datetime, timezone

# 基本配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, 'config.yaml')
SYNC_FILE = os.path.join(BASE_DIR, 'sync.yaml')
CUSTOM_SYNC_FILE = os.path.join(BASE_DIR, 'custom_sync.yaml')
MAX_TAGS_PER_IMAGE = 5  # 每个镜像最多同步的tag数量

def is_exclude_tag(tag):
    """
    更严格的tag过滤规则
    :param tag: 要检查的tag
    :return: True表示需要排除，False表示保留
    """
    # 排除标识列表
    excludes = ['alpha', 'beta', 'rc', 'dev', 'test', 'SNAPSHOT', 'debug', 'nightly']
    
    # 排除特定架构类型
    arch_excludes = ['amd64', 'ppc64le', 'arm64', 'arm', 's390x']
    
    # 规则1: 包含任何排除关键词
    if any(e.lower() in tag.lower() for e in excludes + arch_excludes):
        return True
        
    # 规则2: 长度过长的提交ID (40位的git commit hash)
    if len(tag) >= 40:
        return True
        
    # 规则3: 排除带后缀的tag（保留基本版本号）
    if '-' in tag and not re.search(r"-\d+$", tag):
        return True
        
    # 规则4: 排除日期格式但长度过长的tag
    if re.match(r'^\d{8,}', tag):
        return True
        
    # 规则5: 排除带路径的tag
    if '/' in tag:
        return True
        
    return False


def get_repo_aliyun_tags(image):
    """
    获取 aliuyuncs repo 的tag
    :param image: 镜像名称
    :return: tag列表
    """
    image_name = image.split('/')[-1]
    tags = []

    headers = {
        'User-Agent': 'docker/20.10.7'
    }
    token_url = f"https://dockerauth.cn-hangzhou.aliyuncs.com/auth?scope=repository:ctrimg/{image_name}:pull&service=registry.aliyuncs.com:cn-hangzhou:26842"
    
    try:
        token_res = requests.get(url=token_url, headers=headers, timeout=10)
        token_res.raise_for_status()
        token_data = token_res.json()
        access_token = token_data['token']
    except (requests.RequestException, KeyError) as e:
        print(f'[ERROR] 获取aliyun token失败: {str(e)}')
        return tags

    tag_url = f"https://registry.cn-hangzhou.aliyuncs.com/v2/ctrimg/{image_name}/tags/list"
    headers['Authorization'] = f'Bearer {access_token}'

    try:
        tag_res = requests.get(url=tag_url, headers=headers, timeout=15)
        tag_res.raise_for_status()
        tag_data = tag_res.json()
        tags = tag_data.get('tags', [])
    except requests.RequestException as e:
        print(f'[ERROR] 获取aliyun镜像标签失败: {str(e)}')
    
    print(f'[INFO] aliyun镜像标签: {image_name} -> {len(tags)} tags')
    return tags


def get_repo_gcr_tags(image, limit=MAX_TAGS_PER_IMAGE, host="k8s.gcr.io"):
    """
    获取 gcr.io repo 最新的 tag
    :param host: 仓库主机
    :param image: 镜像名称
    :param limit: 最大标签数量
    :return: tag列表
    """
    headers = {'User-Agent': 'docker/20.10.7'}
    tag_url = f"https://{host}/v2/{image}/tags/list"

    try:
        tag_res = requests.get(url=tag_url, headers=headers, timeout=15)
        tag_res.raise_for_status()
        tag_req_json = tag_res.json()
        manifest_data = tag_req_json.get('manifest', {})
    except requests.RequestException as e:
        print(f'[ERROR] 获取 {host}标签失败: {str(e)}')
        return []

    # 获取镜像详情并排序
    manifest_list = []
    for manifest_id, manifest_info in manifest_data.items():
        tag = manifest_info.get('tag', [None])[0]
        if not tag or is_exclude_tag(tag):
            continue
            
        time_uploaded = manifest_info.get('timeUploadedMs')
        timedate = datetime.utcfromtimestamp(int(time_uploaded)/1000) if time_uploaded else datetime.now(tz=timezone.utc)
        
        manifest_list.append({
            'tag': tag,
            'time': time_uploaded,
            'upload_datetime': timedate
        })

    # 按上传时间排序
    manifest_list.sort(key=lambda x: x['upload_datetime'], reverse=True)
    
    # 获取aliyun已有tag
    aliyun_tags = set(get_repo_aliyun_tags(image))
    
    # 返回未同步的最新tag
    valid_tags = []
    for manifest in manifest_list[:limit]:
        if manifest['tag'] not in aliyun_tags:
            valid_tags.append(manifest['tag'])
    
    print(f'[INFO] {host}镜像标签: {image} -> {len(valid_tags)} tags')
    return valid_tags


def get_repo_quay_tags(image, limit=MAX_TAGS_PER_IMAGE):
    """
    优化获取 quay.io repo 最新的 tag
    :param image: 镜像名称
    :param limit: 最大标签数量
    :return: tag列表
    """
    headers = {'User-Agent': 'docker/20.10.7'}
    page = 1
    all_tags = []
    
    # 分页获取所有活动标签
    while True:
        tag_url = f"https://quay.io/api/v1/repository/{image}/tag/?onlyActiveTags=true&page={page}&limit=100"
        try:
            tag_res = requests.get(url=tag_url, headers=headers, timeout=15)
            tag_res.raise_for_status()
            tag_data = tag_res.json()
            
            tags = tag_data.get('tags', [])
            if not tags:
                break
                
            # 收集非排除标签
            for tag_info in tags:
                tag_name = tag_info.get('name', '')
                if tag_name and not is_exclude_tag(tag_name):
                    all_tags.append({
                        'name': tag_name,
                        'time': tag_info.get('start_ts', 0)
                    })
                    
            # 检查是否有更多页
            next_page = tag_data.get('next_page')
            if not next_page:
                break
                
            page += 1
        except requests.RequestException as e:
            print(f'[ERROR] 获取quay标签失败: {str(e)}')
            break
    
    # 按时间排序
    all_tags.sort(key=lambda x: x['time'], reverse=True)
    
    # 获取aliyun已有tag
    aliyun_tags = set(get_repo_aliyun_tags(image))
    
    # 限制未同步的tag数量
    valid_tags = [
        tag['name'] 
        for tag in all_tags 
        if tag['name'] not in aliyun_tags
    ][:limit]
    
    print(f'[INFO] quay镜像标签: {image} -> {len(valid_tags)} tags')
    return valid_tags


def get_repo_elastic_tags(image, limit=MAX_TAGS_PER_IMAGE):
    """
    优化获取 elastic.io repo 最新的 tag
    :param image: 镜像名称
    :param limit: 最大标签数量
    :return: tag列表
    """
    token_url = f"https://docker-auth.elastic.co/auth?service=token-service&scope=repository:{image}:pull"
    tag_url = f"https://docker.elastic.co/v2/{image}/tags/list"

    headers = {'User-Agent': 'docker/20.10.7'}

    try:
        token_res = requests.get(url=token_url, headers=headers, timeout=10)
        token_res.raise_for_status()
        token_data = token_res.json()
        access_token = token_data['token']
        headers['Authorization'] = f'Bearer {access_token}'
    except (requests.RequestException, KeyError) as e:
        print(f'[ERROR] 获取elastic token失败: {str(e)}')
        return []

    try:
        tag_res = requests.get(url=tag_url, headers=headers, timeout=15)
        tag_res.raise_for_status()
        tag_data = tag_res.json()
        tag_list = tag_data.get('tags', [])
    except requests.RequestException as e:
        print(f'[ERROR] 获取elastic镜像标签失败: {str(e)}')
        return []
    
    # 过滤并排序tag
    valid_tags = [
        tag for tag in tag_list 
        if not is_exclude_tag(tag)
    ]
    valid_tags.sort(key=LooseVersion, reverse=True)
    
    # 获取aliyun已有tag
    aliyun_tags = set(get_repo_aliyun_tags(image))
    
    # 返回未同步的最新tag
    return [
        tag 
        for tag in valid_tags[:limit] 
        if tag not in aliyun_tags
    ]


def get_repo_ghcr_tags(image, limit=MAX_TAGS_PER_IMAGE):
    """
    优化获取 ghcr.io repo 最新的 tag
    :param image: 镜像名称
    :param limit: 最大标签数量
    :return: tag列表
    """
    token_url = f"https://ghcr.io/token?service=ghcr.io&scope=repository:{image}:pull"
    tag_url = f"https://ghcr.io/v2/{image}/tags/list"

    headers = {'User-Agent': 'docker/20.10.7'}

    try:
        token_res = requests.get(url=token_url, headers=headers, timeout=10)
        token_res.raise_for_status()
        token_data = token_res.json()
        access_token = token_data['token']
        headers['Authorization'] = f'Bearer {access_token}'
    except (requests.RequestException, KeyError) as e:
        print(f'[ERROR] 获取ghcr token失败: {str(e)}')
        return []

    try:
        tag_res = requests.get(url=tag_url, headers=headers, timeout=15)
        tag_res.raise_for_status()
        tag_data = tag_res.json()
        tag_list = tag_data.get('tags', [])
    except requests.RequestException as e:
        print(f'[ERROR] 获取ghcr镜像标签失败: {str(e)}')
        return []
    
    # 过滤并排序tag
    valid_tags = [
        tag for tag in tag_list 
        if not is_exclude_tag(tag)
    ]
    valid_tags.sort(key=LooseVersion, reverse=True)
    
    # 获取aliyun已有tag
    aliyun_tags = set(get_repo_aliyun_tags(image))
    
    # 返回未同步的最新tag
    return [
        tag 
        for tag in valid_tags[:limit] 
        if tag not in aliyun_tags
    ]


def get_docker_io_tags(image, limit=MAX_TAGS_PER_IMAGE):
    """
    优化获取 docker hub 仓库标签
    :param image: 镜像名称
    :param limit: 最大标签数量
    :return: tag列表
    """
    namespace_image = image.split('/')
    tag_url = f"https://hub.docker.com/v2/namespaces/{namespace_image[0]}/repositories/{namespace_image[1]}/tags?page=1&page_size=100"
    
    headers = {'User-Agent': 'docker/20.10.7'}
    tags = []
    
    try:
        tag_res = requests.get(url=tag_url, headers=headers, timeout=15)
        tag_res.raise_for_status()
        tag_data = tag_res.json()
        
        # 获取有效的tag
        for tag_info in tag_data.get('results', []):
            tag_name = tag_info.get('name', '')
            if tag_name and not is_exclude_tag(tag_name):
                # 添加大小和时间信息
                size = sum(layer.get('size', 0) for layer in tag_info.get('images', []))
                last_updated = tag_info.get('last_updated', '')
                
                tags.append({
                    'name': tag_name,
                    'size': size,
                    'updated': last_updated
                })
    except requests.RequestException as e:
        print(f'[ERROR] 获取docker.io标签失败: {str(e)}')
        return []
    
    # 按更新时间排序
    tags.sort(key=lambda x: x.get('updated', ''), reverse=True)
    
    # 获取aliyun已有tag
    aliyun_tags = set(get_repo_aliyun_tags(namespace_image[1]))
    
    # 返回未同步的最新tag
    return [
        tag['name'] 
        for tag in tags
        if tag['name'] not in aliyun_tags
    ][:limit]


def get_repo_tags(repo, image, limit=MAX_TAGS_PER_IMAGE):
    """
    统一获取不同仓库标签的函数
    :param repo: 仓库类型
    :param image: 镜像名称
    :param limit: 最大标签数量
    :return: tag列表
    """
    try:
        if repo == 'gcr.io':
            return get_repo_gcr_tags(image, limit, "gcr.io")
        elif repo == 'k8s.gcr.io':
            return get_repo_gcr_tags(image, limit, "k8s.gcr.io")
        elif repo == 'registry.k8s.io':
            return get_repo_gcr_tags(image, limit, "registry.k8s.io")
        elif repo == 'quay.io':
            return get_repo_quay_tags(image, limit)
        elif repo == 'docker.elastic.co':
            return get_repo_elastic_tags(image, limit)
        elif repo == 'ghcr.io':
            return get_repo_ghcr_tags(image, limit)
        elif repo == "docker.io":
            return get_docker_io_tags(image, limit)
        else:
            print(f'[WARNING] 不支持的仓库类型: {repo}')
            return []
    except Exception as e:
        print(f'[ERROR] 获取{repo}仓库{image}标签异常: {str(e)}')
        return []


def generate_dynamic_conf():
    """
    生成动态同步配置，确保每个镜像最多5个tag
    """
    print('[INFO] 开始生成动态同步配置...')
    # 加载配置文件
    try:
        with open(CONFIG_FILE, 'r') as stream:
            config = yaml.safe_load(stream)
    except (IOError, yaml.YAMLError) as e:
        print(f'[ERROR] 加载配置文件失败: {str(e)}')
        return
    
    skopeo_sync_data = {}
    image_count = 0
    
    for repo, images in config.get('images', {}).items():
        if not images:
            continue
            
        skopeo_sync_data.setdefault(repo, {'images': {}})
        
        for image in images:
            print(f"[INFO] 处理动态镜像: {repo}/{image}")
            try:
                # 获取需要同步的标签（自动处理数量限制）
                sync_tags = get_repo_tags(repo, image, config.get('limit', MAX_TAGS_PER_IMAGE))
                
                # 如果获取到有效标签
                if sync_tags:
                    skopeo_sync_data[repo]['images'][image] = sync_tags
                    image_count += 1
                    print(f"[DONE] 找到 {len(sync_tags)} 个新标签: {', '.join(s for s in sync_tags[:3])}{'...' if len(sync_tags) > 3 else ''}")
                else:
                    print(f"[INFO] 没有新标签需要同步: {image}")
            except Exception as e:
                print(f'[ERROR] 处理镜像时出错: {repo}/{image}: {str(e)}')
    
    # 写入结果文件
    try:
        with open(SYNC_FILE, 'w') as f:
            yaml.safe_dump(skopeo_sync_data, f, default_flow_style=False)
        print(f'[SUCCESS] 成功生成动态同步配置: {SYNC_FILE} (包含 {image_count} 个镜像)')
    except IOError as e:
        print(f'[ERROR] 写入同步文件失败: {str(e)}')


def generate_custom_conf():
    """
    生成自定义同步配置，确保每个镜像最多5个tag
    """
    print('[INFO] 开始生成自定义同步配置...')
    
    # 加载自定义配置
    try:
        with open(CUSTOM_SYNC_FILE, 'r') as stream:
            custom_sync_config = yaml.safe_load(stream)
    except (IOError, yaml.YAMLError) as e:
        print(f'[ERROR] 加载自定义配置文件失败: {str(e)}')
        return
    
    custom_skopeo_sync_data = {}
    image_count = 0
    
    for repo, repo_config in custom_sync_config.items():
        if not repo_config or not repo_config.get('images'):
            continue
            
        custom_skopeo_sync_data.setdefault(repo, {'images': {}})
        
        images = repo_config.get('images', {})
        for image, tags in images.items():
            if not tags:
                continue
                
            print(f"[INFO] 处理自定义镜像: {repo}/{image}")
            try:
                # 获取阿里云已有tags
                existing_tags = set(get_repo_aliyun_tags(image))
                
                # 过滤出需要同步的tags
                new_tags = [tag for tag in tags if tag not in existing_tags]
                
                # 数量限制
                if new_tags:
                    # 截取最多5个
                    output_tags = new_tags[:MAX_TAGS_PER_IMAGE]
                    custom_skopeo_sync_data[repo]['images'][image] = output_tags
                    image_count += 1
                    print(f"[DONE] 找到 {len(output_tags)} 个新标签: {', '.join(output_tags[:min(3, len(output_tags))])}{'...' if len(output_tags) > 3 else ''}")
                else:
                    print(f"[INFO] 没有新标签需要同步: {image}")
            except Exception as e:
                print(f'[ERROR] 处理自定义镜像时出错: {repo}/{image}: {str(e)}')
    
    # 写入结果文件
    try:
        with open(CUSTOM_SYNC_FILE, 'w') as f:
            yaml.safe_dump(custom_skopeo_sync_data, f, default_flow_style=False)
        print(f'[SUCCESS] 成功生成自定义同步配置: {CUSTOM_SYNC_FILE} (包含 {image_count} 个镜像)')
    except IOError as e:
        print(f'[ERROR] 写入自定义同步文件失败: {str(e)}')


if __name__ == "__main__":
    print(f'{"="*50}')
    print(f'启动镜像同步清单生成工具 (最大标签数: {MAX_TAGS_PER_IMAGE})')
    print(f'{"="*50}')
    
    # 生成配置
    generate_dynamic_conf()
    print('\n')
    generate_custom_conf()
    
    print(f'\n{"="*50}')
    print('任务执行完毕')
    print(f'{"="*50}\n')
