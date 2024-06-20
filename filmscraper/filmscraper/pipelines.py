# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from filmscraper.items import FilmscraperItem
from filmscraper.items import FilmscraperParsingItem
import psycopg2
import sqlite3
import logging


class FilmscraperPipeline:
    def process_item(self, item, spider):
        parsingAdapter = ItemAdapter(item)
        if parsingAdapter.get('type') == 'raw': 
            filmItem = FilmscraperItem()
            
            filmAdapter = ItemAdapter(filmItem)

            filmItem = self.clean_titre(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_titre_original(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_genre(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_duration(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_annee(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_annee_production(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_nationalite(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_realisateur(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_langues(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_description(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_ratings(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_public(filmItem, parsingAdapter, filmAdapter)
            filmItem = self.clean_acteurs(filmItem, parsingAdapter, filmAdapter)
            logging.info('rentré dans le pipeline 1')
            return filmItem
        return item
    
    def clean_titre(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        titre = parsingAdapter.get('titre')
        filmAdapter['titre'] = titre
        
        return filmItem
    
    def clean_titre_original(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        titre = parsingAdapter.get('titre_original')
        if titre and 'Titre' in titre[-2]:
            value = titre[-1]
        else:
            value = filmAdapter.get('titre')
        filmAdapter['titre_original'] = value
        
        return filmItem
    
    def clean_genre(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        infos = parsingAdapter.get('infos')
        final_pipe_index = max(index for index, item in enumerate(infos) if item == '|')
        filmAdapter['genre'] = infos[final_pipe_index + 1:]
        
        return filmItem
        
    def clean_duration(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        infos = parsingAdapter.get('duration')
        if all(element == '\n' for element in infos):
            value = 'N/A'
        else:
            value = next(value for value in infos if value != '\n').replace('\n', '')
        filmAdapter['duration'] = value
        
        return filmItem
    
    def clean_annee(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        infos = parsingAdapter.get('infos')
        annee_sortie = infos[0].replace('\n', '').split(' ')[-1]
        filmAdapter['annee_sortie'] = annee_sortie
        
        return filmItem
    
    def clean_annee_production(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        infos_technique = parsingAdapter.get('infos_technique')
        annee_production = infos_technique[infos_technique.index("Année de production") + 1]
        filmAdapter['annee_production'] = annee_production
        
        return filmItem
        
    def clean_nationalite(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        nationalite = parsingAdapter.get('nationalite')
        filmAdapter['nationalite'] = nationalite
        
        return filmItem
    
    def clean_realisateur(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        realisateur = parsingAdapter.get('realisateur')
        tmp = parsingAdapter.get('only_realisateur')
        if realisateur:
           real = realisateur[1:realisateur.index('Par')]
        elif tmp:
            real = tmp[1:]
        else:
            real = 'N/A'
        filmAdapter['realisateur'] = real
        return filmItem
    
    def clean_langues(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        infos_technique = parsingAdapter.get('infos_technique')
        langues = list(string.replace('\n', '') for string in infos_technique[infos_technique.index("Langues") + 1: infos_technique.index("Format production")])
        filmAdapter['langues'] = langues
        
        return filmItem
    
    def clean_description(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        description = parsingAdapter.get('description')
        filmAdapter['description'] = description
        
        return filmItem
    
    def clean_ratings(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        ratings = parsingAdapter.get('ratings')
        if len(ratings) == 2:
            filmAdapter['notes_presse'] = ratings[0]
            filmAdapter['notes_spectateur'] = ratings[1]
        else:
            filmAdapter['notes_presse'] = None
            filmAdapter['notes_spectateur'] = ratings[0]
        
        return filmItem
    
    def clean_public(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        public = parsingAdapter.get('public')
        
        filmAdapter['public'] = public if public is not None else 'N/A'
        
        return filmItem
    
    def clean_acteurs(self,filmItem: FilmscraperItem, parsingAdapter: ItemAdapter, filmAdapter: ItemAdapter):
        acteurs = parsingAdapter.get('acteurs')
        acteurs = list(acteur for acteur in acteurs if '\n' not in acteur)
        if len(acteurs) > 8:
            filmAdapter['acteurs'] = acteurs[:7]
        else:
            filmAdapter['acteurs'] = acteurs
        
        return filmItem
    
class FilmDatabasePipeline:
    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = ''
        database = "postgres"
        
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        
        self.cur = self.connection.cursor()
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS films(
            id SERIAL PRIMARY KEY,
            titre TEXT,
            titre_original TEXT,
            genre TEXT[],
            duration TEXT,
            annee_sortie INTEGER,
            annee_production INTEGER,
            nationalite TEXT[],
            langues TEXT[],
            realisateur TEXT,
            description TEXT,
            notes_presse NUMERIC,
            notes_spectateur NUMERIC,
            public TEXT,
            acteurs TEXT[]
        )
        ''')
        self.connection.commit()
        
    def process_item(self, item, spider):
        if item.get('type') == "clean":
            self.cur.execute('''
                INSERT INTO films(
                titre,
                titre_original,
                genre,
                duration,
                annee_sortie,
                annee_production,
                nationalite,
                langues,
                realisateur,
                description,
                notes_presse,
                notes_spectateur,
                public,
                acteurs
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)''', (item['titre'], item['titre_original'], item['genre'], item['duration'], 
                                                        item['annee_sortie'], item['annee_production'], item['nationalite'], item['langues'],
                                                        item['realisateur'], item['description'], item['notes_presse'], item['notes_spectateur'],
                                                        item['public'], item['acteurs'])
                )
            self.connection.commit()
            return item
        pass
    
    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()