""" Ce programme permet la lecture et le nettoyage du fichier LivresAuteursLangues.txt"""
 #|OUTPUT_PROTOCOL = BytesValueProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import BytesValueProtocol
class Ev3Partie1(MRJob):
      OUTPUT_PROTOCOL = BytesValueProtocol
      def steps(self):
        return [
            MRStep(mapper=self.mapper_get_Lecture1,
                   reducer=self.reducer_Nettoyage1) 
            #,
                # MRStep( reducer=self.reducer_Numerotation1 )          
                 ]
    
    #Lecture du fichier LivresAuteursLangues.txt
      def mapper_get_Lecture1(self, _, line):   
            yield None, line
    #
      def reducer_Nettoyage1(self, _,livres):
          for livre in list(livres):
              yield _, bytes(livre, 'utf-8')
              
      #  for element in AuteursLangues:
         #   if len(element)==1:
           #     break
       # NomAuteur=element
       # try :
         #   ListeLivre.remove(element) 
        #except(ValueError) :
           #  pass
       # for element in ListeLivre:

    # 
   # def reducer_Numerotation1 (self, film, titresEvals):
          # titresEvals = [x for x in titresEvals]
          # yield film, titresEvals

if __name__ == '__main__':
         Ev3Partie1.run()
