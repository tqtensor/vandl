import os
import pickle
import zipfile

from selenium import webdriver


def get_chromedriver(use_proxy=False, user_agent=None,
                     PROXY_HOST='nordvpn', PROXY_PORT=80,
                     PROXY_USER='user', PROXY_PASS='pwd'):
    path = os.path.dirname(os.path.abspath(__file__))
    chrome_options = webdriver.ChromeOptions()

    # Inject credentials to chrome driver
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = """
    var config = {
            mode: "fixed_servers",
            rules: {
            singleProxy: {
                scheme: "http",
                host: "%s",
                port: parseInt(%s)
            },
            bypassList: ["localhost"]
            }
        };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
    );
    """ % (PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS)

    if use_proxy:
        pluginfile = 'proxy_auth_plugin.zip'

        with zipfile.ZipFile(pluginfile, 'w') as zp:
            zp.writestr("manifest.json", manifest_json)
            zp.writestr("background.js", background_js)
        chrome_options.add_extension(pluginfile)
    if user_agent:
        chrome_options.add_argument('--user-agent=%s' % user_agent)

    driver = webdriver.Chrome(
        os.path.join(path, 'chromedriver'),
        chrome_options=chrome_options)
    return driver


def loadtime_calc(PROXY_HOST, PROXY_PORT,
                  PROXY_USER, PROXY_PASS):
    url = "https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml"
    driver = get_chromedriver(use_proxy=True, PROXY_HOST=PROXY_HOST,
                              PROXY_PORT=PROXY_PORT, PROXY_USER=PROXY_USER,
                              PROXY_PASS=PROXY_PASS)
    driver.implicitly_wait(20)
    try:
        driver.get(url)
    except Exception as e:
        print(getattr(e, 'message', repr(e)))

    navigationStart = driver.execute_script(
        "return window.performance.timing.navigationStart")
    responseStart = driver.execute_script(
        "return window.performance.timing.responseStart")
    domComplete = driver.execute_script(
        "return window.performance.timing.domComplete")

    backendPerformance_calc = responseStart - navigationStart
    frontendPerformance_calc = domComplete - responseStart

    driver.quit()
    return backendPerformance_calc + frontendPerformance_calc


def get_proxy():
    proxy_server = ['nl490.nordvpn.com', 'kr23.nordvpn.com',
                    'id8.nordvpn.com', 'hk142.nordvpn.com']
    if os.path.exists('proxy_credential.pickle'):
        with open('proxy_credential.pickle', 'rb') as f:
            proxy_credential = pickle.load(f)
            print(proxy_credential)

        # Test the speed of each proxy server
        bad_credentials = []

        min_loadtime = 10**6
        for proxy in proxy_server:
            for credential in proxy_credential:
                PROXY_HOST = proxy
                PROXY_PORT = 80
                PROXY_USER = credential[0]
                PROXY_PASS = credential[1]

                if PROXY_USER not in bad_credentials:
                    loadtime = loadtime_calc(PROXY_HOST, PROXY_PORT,
                                             PROXY_USER, PROXY_PASS)
                    if loadtime > 40000:
                        bad_credentials.append(PROXY_USER)

                    print(
                        f'>>> Tested proxy {PROXY_HOST} with user: {PROXY_USER} and pwd: {PROXY_PASS}; loadtime: {loadtime}')

                    if loadtime <= min_loadtime:
                        min_loadtime = loadtime
                        proxy_config = [PROXY_HOST, PROXY_PORT,
                                        PROXY_USER, PROXY_PASS]

        print(f'>>> Best proxy server and credential: {proxy_config}')
        with open('proxy_config.pickle', 'wb') as f:
            pickle.dump(proxy_config, f)
    else:
        print('>>> You need the login account for NordVPN')
        print('>>> For now the program will not use proxy')
        with open('proxy_config.pickle', 'wb') as f:
            pickle.dump([], f)
        pass


def return_proxy():
    with open('proxy_config.pickle', 'rb') as f:
        proxy_config = pickle.load(f)

    if not proxy_config:
        driver = get_chromedriver(use_proxy=False)
        return driver
    else:
        PROXY_HOST = proxy_config[0]
        PROXY_PORT = proxy_config[1]
        PROXY_USER = proxy_config[2]
        PROXY_PASS = proxy_config[3]
        driver = get_chromedriver(use_proxy=True, PROXY_HOST=PROXY_HOST,
                                  PROXY_PORT=PROXY_PORT, PROXY_USER=PROXY_USER,
                                  PROXY_PASS=PROXY_PASS)
        return driver


if __name__ == "__main__":
    get_proxy()
