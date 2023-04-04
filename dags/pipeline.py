from airflow import DAG
import os
from datetime import datetime
#operador que vai executar a ação
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator   
from airflow.models.xcom import XCom 
from airflow.operators.dummy_operator import DummyOperator
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from time import sleep
from selenium.webdriver.common.by import By
import pandas as pd
from selenium.webdriver.support.wait import WebDriverWait
import numpy as np
from elasticsearch import Elasticsearch
import requests
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities



def acessar_pagina():
    option = Options()
    option.add_argument('--disable-blink-features=AutomationControlled') 
    option.add_argument('--disable-extensions')
    option.add_argument('--headless')
    option.add_argument('--disable-gpu')
    option.add_argument('--no-sandbox')
    option.add_argument('--disable-dev-shm-usage')
        
    option.add_experimental_option('useAutomationExtension', False)
    option.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    acesso_pagina = webdriver.Remote(
            command_executor='http://chrome:4444/wd/hub', desired_capabilities=DesiredCapabilities.CHROME,options=option)
    acesso_pagina.get('https://sac.sefaz.mt.gov.br/citsmart/pages/knowledgeBasePortal/knowledgeBasePortal.load#/list/1')
    
    sleep(52)
    
    return acesso_pagina  
  
all_texts = []
def processar_pagina(pagina = [],contador = 1):    
    
    if contador > 4:
        return all_texts     
    
    if not pagina:
        pagina = acessar_pagina()
               
    #page = pagina.page_source #tirar um print da página pegando o html  
    search_area = BeautifulSoup(pagina.page_source, 'html.parser')   
    hospedagens = search_area.find_all('div', attrs={'class':'knowledge-base-list-item clearfix ng-scope'})
        
    for hospedagem in hospedagens:
        all = {}
        try:
            u = 'https://sac.sefaz.mt.gov.br/citsmart/pages/knowledgeBasePortal/knowledgeBasePortal.load'
            a = hospedagem.find('a', attrs={'ui-sref':'knowledgeBase.knowledge({idKnowledge: knowledge.idBaseConhecimento})'})
            url = u+a['href']
            all = url    
        except:
            all = ["Vazio"]
            
        all_texts.append(all)
    print(len(all_texts))
    sleep(46)                    
    ultima_pagina = pagina.find_element(By.CSS_SELECTOR, 'div.knowledge-base-pagination.margin-top.clearfix > ul > li.pagination-next.ng-scope > a')
    ultima_pagina.click()
            
    sleep(52)
        
    contador = contador + 1
                  
    return processar_pagina(pagina, contador)
   
 
###############################################################################
## Scrapp dos artigos 
###############################################################################
geral = []

def scrapp_artigo_completo(ti):
    all_texts = ti.xcom_pull(task_ids = 'scrapp_dos_artigos')
    for dados in range(len(all_texts)):
        link = all_texts[dados]
        print(link)
                
        option = Options()
        option.add_argument('--headless')
                      
        url = webdriver.Remote(command_executor='http://chrome:4444/wd/hub', options=option) 
        url.get(link)

        sleep(12)

        article_area = BeautifulSoup(url.page_source, 'html.parser')
        article_class1 = article_area.find_all('div', attrs={'id':'knowledge-base-content'})
                           
        for Steps_article in article_class1:
            article = {}
            try:
                article['URL'] = link   
            except:
                article['URL'] = ["Vazio"]
            try:
                t = Steps_article.find('h2', attrs={'class':'knowledge-base-item-title margin-less pull-left break-word ng-binding'}).get_text()
                article['Titulo'] = t
            except:
                article['Titulo'] = ['Vazio']
            try:
                e = Steps_article.find('div', attrs={'class':'knowledge-base-item-text hyphens ng-binding'}).get_text().strip()
                article['Texto'] = e
            except: 
                article['Texto'] = ["Vazio"]                        
            geral.append(article)
            
        url.close()            
        
    excluir_vazio(geral)
      
    return geral

###############################################################################
## Tratamento dos dados
###############################################################################
rest = []
 
 #Excluir os itens = ['Vazio'] 
def excluir_vazio(geral):
    K =  ['Vazio']  
            
    rest = [sub for sub in geral if K not in list(sub.values())]
          
    import_files(rest)
            
    print("\nArquivo tratado\n")
###############################################################################
#Download do artigo - texto no Elasticsearch

def import_files(rest):
    for artigo in range(len(rest)):
        artigo_process = rest[artigo]
                   
        # Converter a lista para o formato json
        import json
        artigo1 = json.dumps(artigo_process)
        
        es = Elasticsearch(
            hosts="https://localhost:9200",
            ca_certs="C:/Users/adria/Documents/Trabalho - Central/codigos/http_ca.crt",
            http_auth=("elastic", "WTDxSVLE0Wx-EK4Ngoh="),
            verify_certs=True
            )
                 
        #salvar o artigo
        res = es.index(index="import_files", document=artigo1)

        print("\n1. Texto do artigo importado\n",res)          


#def e_valida(ti):
#    all_texts = ti.xcom_pull(task_ids = 'scrapp_dos_artigos')
#    if len(all_texts) >= 600:
#        print(len(all_texts))
#        return 'scrapp_artigo_completo'
#    else:
#        return 'nvalido'
        
    
default_arguments = {
    'owner': 'buscadorElastic',
    'start_date': datetime(2023,3, 17),
}

with DAG(dag_id = 'buscador', start_date= datetime(2023,3, 20), schedule='*/30 */12 * * *', default_args=default_arguments,catchup=False) as dag:
    scrapp_dos_artigos_dag = PythonOperator(
        task_id='scrapp_dos_artigos',
        python_callable=processar_pagina
    )
#    e_valida_dag = BranchPythonOperator(
#        task_id='e_valida',
#        python_callable=e_valida
#    )
    
    #Verificar a quantidade de itens na lista
    #valido_dag = BashOperator(
    #    task_id = 'valido',
    #    bash_command = "echo 'Quantidade OK'"
    #)
#    nvalido_dag = BashOperator(
#        task_id= 'nvalido',
#        bash_command = "echo 'Quantidade N OK'"
#    )
        
    scrapp_artigo_completo_dag = PythonOperator(
        task_id='scrapp_artigo_completo',
        python_callable=scrapp_artigo_completo   
    )
    
       
    end = DummyOperator(
        task_id= 'end'
    )
        
    scrapp_dos_artigos_dag >> scrapp_artigo_completo_dag >> end
    