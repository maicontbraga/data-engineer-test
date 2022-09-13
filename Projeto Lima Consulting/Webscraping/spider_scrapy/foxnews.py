import scrapy
from scrapy.selector import Selector
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time

#Script que acessa a url do portal de notícias foxnews, navega até o ambiente de economia e raspa os dados das notícias do portal

# Para a execução deste bloco de código, precisará ser executado no terminal o seguinte comando: $scrapy crawl foxnews -o output_scrapy.json

# É importante dar uma olhada nas libraries instaladas para execução do script no arquivo requirements.txt


#Classe Spider para execução do scrapy
class FoxnewsSpider(scrapy.Spider):
    name = 'foxnews'
    allowed_domains = ['www.foxnews.com']
    start_urls = ['https://www.foxnews.com']


    #Função que instancia o chrome como webdriver, acessa a página de economia da foxnews e configura a página para o scraping
    def __init__(self):
        options = Options()
        driver = webdriver.Chrome(executable_path=str('/Users/maicon.braga/Downloads/chromedriver'), options=options)
        driver.get("https://www.foxnews.com/category/us/economy")

        wait = WebDriverWait(driver, 10)
        
        # Ele entrará na página de economia e clicará em "show more" 50 vezes
        i = 0
        while i < 50:
            try:
                time.sleep(1)

        # Estrutura de dependencias para aguardar novamente o botão do "show more" aparecer antes de clicar
                element = wait.until(EC.visibility_of_element_located(
                    (By.XPATH, "(//div[@class='button load-more js-load-more'])[1]/a")))
                element.click()
                i += 1
            except TimeoutException:
                break
                
        # Após clicar em "show more" 50 vezes, ele armazena os dados de todas as notíficas
        self.html = driver.page_source


    # Funções para localizar o título da notícia, autor, data e conteúdo da notícia, e armazenamento em um json
    def parse(self, response):
        resp = Selector(text=self.html)
        results = resp.xpath("//article[@class='article']//h4[@class='title']/a")
        for result in results:
            title = result.xpath(".//text()").get()
            eyebrow = result.xpath(".//span[@class='eyebrow']/a/text()").get()
            link = result.xpath(".//@href").get()
            if eyebrow == 'VIDEO':
                continue 
            else:
                yield response.follow(url=link, callback=self.parse_article, meta={"title": title})

    def parse_article(self, response):
        title = response.request.meta['title']
        authors = response.xpath("(//div[@class='author-byline']//span/a)[1]/text()").getall()
        if len(authors) == 0:
            authors = [i for i in response.xpath("//div[@class='author-byline opinion']//span/a/text()").getall() if 'Fox News' not in i]
        content = ' '.join(response.xpath("//div[@class='article-body']//text()").getall())
        yield {
            "title": title,
            "byline": ' '.join(authors),
            "time": response.xpath("//div[@class='article-date']/time/text()").get(),
            "content": content
        }