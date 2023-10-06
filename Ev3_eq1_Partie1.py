""" Ce programme permet la lecture et le nettoyage du fichier LivresAuteursLangues.txt"""
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import BytesValueProtocol
class Ev3Partie1(MRJob):
    OUTPUT_PROTOCOL =BytesValueProtocol
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_Lecture,
                   reducer=self.reducer_Nettoyage1) ,
                 MRStep( reducer=self.reducer_Numerotation1 )            ]

    #Lecture du fichier LivresAuteursLangues.txt
    def mapper_get_Lecture1(self, _, line):
        
        #Lecture du fichier LivresAuteursLangues.txt
         LivresAuteursLangues = line.split("-")
         Titre=LivresAuteursLangues[0]
         Auteur=LivresAuteursLangues[1]
         Langue=LivresAuteursLangues[2]
         yiel
#
    def reducer_Nettoyage1(self, film, titresEvals):
           titresEvals = [x for x in titresEvals]
           yield film, titresEvals

# 
def reducer_Numerotation1 (self, film, titresEvals):
           titresEvals = [x for x in titresEvals]
           yield film, titresEvals

if __name__ == '__main__':
    Ev3Partie1.run()
