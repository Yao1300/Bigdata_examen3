""" Ce programme permet la lecture et le nettoyage du fichier LivresAuteursLangues.txt"""
from mrjob.job import MRJob
from mrjob.step import MRStep
class Ev3Partie1(MRJob):
      def steps(self):
        return [
            MRStep(mapper=self.mapper_get_Lecture1,
                   reducer=self.reducer_Nettoyage1) 
            #,
             #  MRStep( reducer=self.reducer_Numerotation1 )          
                 ]
    
    #Lecture du fichier LivresAuteursLangues.txt
      def mapper_get_Lecture1(self, _, line): 
             line = line.replace("- Auteur inconnu (vous pouvez préciser l'auteur si vous le connaissez)" ,",")  
             line = line.replace("- Inconnu titre original-",",")  
             line = line.replace("- Langue originale : Inconnue",",") 
             line = line.replace("- Titre original : Inconnu",",") 
             line = line.replace("- Langue originale : Français - Titre original",",") 
             line = line.replace("-",",")
             yield None,line
    #
      def reducer_Nettoyage1(self,numero,livres):
         # for livre in list(livres):
          numero = 1
          for livre in list(livres):
              numero = numero + 1
              yield numero, bytes(livre, 'utf-8')
if __name__ == '__main__':
         Ev3Partie1.run()