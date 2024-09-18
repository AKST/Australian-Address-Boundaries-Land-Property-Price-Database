def url_with_params(url, params):
    from urllib.parse import urlparse, urlencode
    return f"{url}?{urlencode(params)}"

def url_host(url):
    from urllib.parse import urlparse
    return urlparse(url).netloc