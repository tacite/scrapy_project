# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field
import scrapy.item

class FilmscraperParsingItem(scrapy.Item):
    titre = Field()
    titre_original = Field()
    infos = Field()
    infos_technique = Field()
    realisateur = Field()
    only_realisateur = Field()
    nationalite = Field()
    description = Field()
    ratings = Field()
    duration = Field()
    public = Field()
    acteurs = Field()
    type = "raw"
    
class FilmscraperItem(scrapy.Item):
    titre = Field()
    titre_original = Field()
    genre = Field()
    duration = Field()
    annee_sortie = Field()
    annee_production = Field()
    nationalite = Field()
    realisateur = Field()
    langues = Field()
    description = Field()
    notes_presse = Field()
    notes_spectateur = Field()
    public = Field()
    acteurs = Field()
    type = "clean"