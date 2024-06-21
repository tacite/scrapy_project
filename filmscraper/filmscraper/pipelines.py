# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from filmscraper.items import FilmscraperItem, SeriescraperItem
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
            filmAdapter['type'] = "clean"

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
            return filmItem
        return None
    
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
            filmAdapter['notes_presse'] = ratings[0].replace(',', '.')
            filmAdapter['notes_spectateur'] = ratings[1].replace(',', '.')
        else:
            filmAdapter['notes_presse'] = None
            filmAdapter['notes_spectateur'] = ratings[0].replace(',', '.')
        
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
        self.cur.execute("DROP TABLE films")
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
        adapter = ItemAdapter(item)
        if adapter.get('type') == "clean":
            notes_presse = float(adapter.get('notes_presse')) if adapter.get('notes_presse') is not None else None
            notes_spectateur = float(adapter.get('notes_spectateur')) if adapter.get('notes_spectateur') is not None else None
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
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)''', (
                    adapter.get('titre'),
                    adapter.get('titre_original'),
                    adapter.get('genre'),
                    adapter.get('duration'),
                    adapter.get('annee_sortie'),
                    adapter.get('annee_production'),
                    adapter.get('nationalite'),
                    adapter.get('langues'),
                    adapter.get('realisateur'),
                    adapter.get('description'),
                    notes_presse,
                    notes_spectateur,
                    adapter.get('public'),
                    adapter.get('acteurs')
                )
            )
            self.connection.commit()
            return item
        pass
    
    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()
        
        
class SeriescraperPipeline:
    def process_item(self, item, spider):
        parsingAdapter = ItemAdapter(item)
        serieItem = SeriescraperItem()
        serieAdapter = ItemAdapter(serieItem)
        
        serieItem = self.clean_titre(serieItem, parsingAdapter, serieAdapter)
        serieItem = self.clean_body_info(serieItem, parsingAdapter, serieAdapter)
        serieItem = self.clean_body_direction(serieItem, parsingAdapter, serieAdapter)
        serieItem = self.clean_body_nationalite(serieItem, parsingAdapter, serieAdapter)
        serieItem = self.clean_body_titre_original(serieItem, parsingAdapter, serieAdapter)
        serieItem = self.clean_ratings(serieItem, parsingAdapter, serieAdapter)
        serieItem = self.clean_description(serieItem, parsingAdapter, serieAdapter)
        serieItem = self.clean_saison_episode(serieItem, parsingAdapter, serieAdapter)
        serieItem = self.clean_status(serieItem, parsingAdapter, serieAdapter)
        
        return serieItem
    
    def clean_titre(self,serieItem: SeriescraperItem, parsingAdapter: ItemAdapter, serieAdapter: ItemAdapter):
        titre = parsingAdapter.get('titre')
        serieAdapter['titre'] = titre
        
        return serieItem
    
    def clean_body_info(self,serieItem: SeriescraperItem, parsingAdapter: ItemAdapter, serieAdapter: ItemAdapter):
        lst = parsingAdapter.get('body_info')
        lst = self.clean_list(lst)
        count_pipe = lst.count('|')
        match count_pipe:
            case 1:
                serieAdapter['periode'] = lst[0]
                serieAdapter['duree_moyenne'] = 'N/A'
                serieAdapter['genre'] = lst[2:]
            case 2:
                serieAdapter['periode'] = lst[0]
                serieAdapter['duree_moyenne'] = lst[2]
                serieAdapter['genre'] = lst[4:]
        
        return serieItem
    
    def clean_body_direction(self,serieItem: SeriescraperItem, parsingAdapter: ItemAdapter, serieAdapter: ItemAdapter):
        lst = parsingAdapter.get('body_direction')
        if lst:
            lst = self.clean_list(lst)
            serieAdapter['createur'] = lst[1:]
        else:
            serieAdapter['createur'] = []
        
        return serieItem
    
    def clean_body_nationalite(self,serieItem: SeriescraperItem, parsingAdapter: ItemAdapter, serieAdapter: ItemAdapter):
        lst = parsingAdapter.get('body_nationality')
        if lst:
            lst = self.clean_list(lst)
            if 'En relation avec' in lst:
                relation_index = lst.index('En relation avec')
                serieAdapter['nationalite'] = lst[1:relation_index]
            else:
                serieAdapter['nationalite'] = lst[1:]
        else:
            serieAdapter['nationalite'] = []
        
        return serieItem
    
    def clean_body_titre_original(self,serieItem: SeriescraperItem, parsingAdapter: ItemAdapter, serieAdapter: ItemAdapter):
        lst = parsingAdapter.get('body_titre_original')
        if lst:
            lst = self.clean_list(lst)
            serieAdapter['titre_original'] = lst[1]
        else:
            serieAdapter['titre_original'] = serieAdapter.get('titre')
        
        return serieItem
    
    def clean_ratings(self,serieItem: SeriescraperItem, parsingAdapter: ItemAdapter, serieAdapter: ItemAdapter):
        ratings = parsingAdapter.get('ratings')
        if len(ratings) == 2:
            serieAdapter['notes_presse'] = ratings[0].replace(',', '.')
            serieAdapter['notes_spectateur'] = ratings[1].replace(',', '.')
        elif len(ratings) == 1:
            serieAdapter['notes_presse'] = None
            serieAdapter['notes_spectateur'] = ratings[0].replace(',', '.')
        else:
            serieAdapter['notes_presse'] = None
            serieAdapter['notes_spectateur'] = None
            
        return serieItem
    
    def clean_description(self,serieItem: SeriescraperItem, parsingAdapter: ItemAdapter, serieAdapter: ItemAdapter):
        description = parsingAdapter.get('description')
        
        if description:
            serieAdapter['description'] = description[0]
        else:
            serieAdapter['description'] = []    
            
        return serieItem
    
    def clean_saison_episode(self,serieItem: SeriescraperItem, parsingAdapter: ItemAdapter, serieAdapter: ItemAdapter):
        lst = parsingAdapter.get('saison_episode')
        match len(lst):
            case 1:
                serieAdapter['saisons'] = lst[0].split(' ')[0]
            case 2:
                serieAdapter['saisons'] = lst[0].split(' ')[0]
                serieAdapter['episodes'] = lst[1].split(' ')[0]
            case 3:
                serieAdapter['saisons'] = lst[0].split(' ')[0]
                serieAdapter['episodes'] = lst[1].split(' ')[0]
                serieAdapter['prix'] = lst[2].split(' ')[0]
                
        
        return serieItem
    
    def clean_status(self,serieItem: SeriescraperItem, parsingAdapter: ItemAdapter, serieAdapter: ItemAdapter):
        status = parsingAdapter.get('status')
        
        serieAdapter['status'] = status
        
        return serieItem
    
    def clean_list(self, lst: list) -> list:
        
        def filter_list(string: str) -> bool:
            if string == '' or (len(string) == 1 and string != '|'):
                return False
            return True
        
        lst = list(element.replace('\n', '') for element in lst)
        lst = list(filter(filter_list, lst))
        return lst
    
    
class SerieDatabasePipeline:
    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = ''
        database = "postgres"
        
        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        
        self.cur = self.connection.cursor()
        self.cur.execute("DROP TABLE series")
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS series(
            id SERIAL PRIMARY KEY,
            titre TEXT,
            titre_original TEXT,
            createur TEXT[],
            periode TEXT,
            duree_moyenne TEXT,
            nationalite TEXT[],
            genre TEXT[],
            notes_presse NUMERIC,
            notes_spectateur NUMERIC,
            description TEXT,
            saisons TEXT,
            episodes TEXT,
            prix TEXT,
            status TEXT
        )
        ''')
        self.connection.commit()
        
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        notes_presse = float(adapter.get('notes_presse')) if adapter.get('notes_presse') is not None else None
        notes_spectateur = float(adapter.get('notes_spectateur')) if adapter.get('notes_spectateur') is not None else None
        self.cur.execute('''
            INSERT INTO series(
            titre,
            titre_original,
            createur,
            periode,
            duree_moyenne,
            nationalite,
            genre,
            notes_presse,
            notes_spectateur,
            description,
            saisons,
            episodes,
            prix,
            status
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)''', (
                adapter.get('titre'),
                adapter.get('titre_original'),
                adapter.get('createur'),
                adapter.get('periode'),
                adapter.get('duree_moyenne'),
                adapter.get('nationalite'),
                adapter.get('genre'),
                notes_presse,
                notes_spectateur,
                adapter.get('description'),
                adapter.get('saisons'),
                adapter.get('episodes'),
                adapter.get('prix'),
                adapter.get('status')
            )
        )
        self.connection.commit()
        return item
    
    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()