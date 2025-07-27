# PRIMEIRA ETAPA: Vamos importar as bibliotecas, elas que vão tratar os dados HTML que vamos importar.

#Estas importações nos dão ferramentas para pegar dados da web, tratar o conteúdo HTML, e realizar tarefas de forma paralela além de salvar os resultados em um arquivo.

import requests # Para requisições em HTTP
import time # Para cálculo de tempo e simular atrasos(delays)
import csv # Para salvar os resultados em formato de texto .CSV
import random # Para adicionar o recurso de aleatoriedade
import concurrent.futures # Para conseguir rodar threads em paralelo
from bs4 import BeautifulSoup # Para conseguir "navegar" no arquivo HTML

#-----------------------------------------------------------------------------------------------------------------------------------

# SEGUNDA ETAPA: Agora vamos "enganar" o site do IMDB, vamos simular que o nosso código é um navegador acessando o site.
# O headers tem a tarefa de enganar o site e fazer parecer que somos um navegador real. Sem isso, o IMDb pode bloquear a requisição.

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
}

MAX_THREADS = 15 # O MAX_THREADS define quantas requisições vão ser realizadas ao mesmo tempo.

#-----------------------------------------------------------------------------------------------------------------------------------

# TERCEIRA ETAPA: Hora de escrever a função que vai extrair os detalhes dos filmes.
# A função será executada uma vez para cada filme que chamarmos. Vamos fazer uso do BeautifulSoup que importamos para poder ler o HTML da página que estamos acessando.

def extract_movie_details(movie_link):
    # Aqui usamos o time para criar um delay e evitar bloqueio por scraping
    time.sleep(random.uniform(0, 0.2))

    response = requests.get(movie_link, headers=headers)
    movie_soup = BeautifulSoup(response.content, 'html.parser')

#-----------------------------------------------------------------------------------------------------------------------------------

# QUARTA ETAPA: A função desta etapa de código é tentar encontrar a seção do HTML aonde estão os dados principais do filme.
# OBS: A estrutura do IMDb muda com frequência, por isso é comum a necessidade de atualizar as classes.

    if movie_soup is not None:
        title = None
        date = None
        # Buscando a seção principal da página para conseguir as informações do filme
        page_section = movie_soup.find('section', attrs={'class': 'ipc-page-section'})

#-----------------------------------------------------------------------------------------------------------------------------------

# QUINTA ETAPA: Aqui vamos buscar as divs dentro da seção, vamos afirmar que a segunda div vai ter os dados(índice 1).
# Pode ser que isso mude com o tempo, então não é garantido.

        if page_section is not None:
            divs = page_section.find_all('div', recursive=False)

            if len(divs) > 1:
                target_div = divs[1]

#-----------------------------------------------------------------------------------------------------------------------------------

# SEXTA ETAPA: Precisamos conseguir o título do filme, pela estrutura do site que estamos analisando, ele vai estar em um span, dentro de um h1.
# vamos usar o .get_text() para extrair só o texto que precisamos. 

                title_tag = target_div.find('h1')
                if title_tag:
                    title = title_tag.find('span').get_text()

#-----------------------------------------------------------------------------------------------------------------------------------

# SETIMA ETAPA: Obter a data de lançamento do filme. Pela estrutura da página, esta informação vai estar em um link que leva para a página "informações de lançamento".
# Então vamos atrás deste href para obter a informação.

                date_tag = target_div.find('a', href=lambda href: href and 'releaseinfo' in href)
                if date_tag:
                    date = date_tag.get_text().strip()

#-----------------------------------------------------------------------------------------------------------------------------------

# OITAVA ETAPA: Vamos conseguir a nota dos filmes, elas são armazenadas em uma div com um atributo data-testid. Vamos usar essa informação para buscar o que precisamos.

                rating_tag = movie_soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
                rating = rating_tag.get_text() if rating_tag else None

#-----------------------------------------------------------------------------------------------------------------------------------

# NONA ETAPA: E para finalizar a obtenção de dados, vamos pegar a sinopse. Assim como fizemos com a nota, vamos usar o data-testid para encontrar a sinopse do filme.

                plot_tag = movie_soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
                plot_text = plot_tag.get_text().strip() if plot_tag else None

#-----------------------------------------------------------------------------------------------------------------------------------

# DÉCIMA ETAPA: Já conseguimos todas as informações que precisamos, agora temos que salvar os dados dentro de um arquivo CSV.
# Usamos um With open com mode='a', para que a cada ativação o conteúdo eja adicionado em append, e não sobrescrito.

                with open('movies.csv', mode='a', newline='', encoding='utf-8') as file:
                    movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    if all([title, date, rating, plot_text]):
                        print(title, date, rating, plot_text)
                        movie_writer.writerow([title, date, rating, plot_text])

# Com isso, temos a função de extração das informações do filme completa. Vamos para a contrução da função principal.

#-----------------------------------------------------------------------------------------------------------------------------------

# DÉCIMA PRIMEIRA ETAPA: Vamos criar uma função que vai extrair os filmes da página e fazer a função que acabamos de criar passar por cada um deles.
# Assim montando nosso arquivo CSV. Nesta parte, com a ajuda de multithreading vamos baixar os dados em paralelo, acelerando o processo.

def extract_movies(soup):
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')

    # Pega os links dos filmes
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    # Usa múltiplas threads pra acelerar o scraping
    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(extract_movie_details, movie_links)

#-----------------------------------------------------------------------------------------------------------------------------------

#DÉCIMA SEGUNDA ETAPA: Hora de criar nossa função main! Ela vai pegar a página dos filmes populares, converter em arquivo soup e enviar para a função extract_movies().
# No fim da execução vai nos mostrar quanto tempo levou para executar a função.

def main():
    start_time = time.time()

    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    extract_movies(soup)

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)

if __name__ == '__main__':
    main()

# Este último if faz com que o Python execute a função main() apenas se o script for rodado diretamente, e não se for importado por outro código.
