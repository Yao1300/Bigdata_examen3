""" Ce programme permet la lecture et le nettoyage du fichier LivresAuteursLangues.txt"""
 #|OUTPUT_PROTOCOL = BytesValueProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import BytesValueProtocol
class Ev3Partie2(MRJob):
      OUTPUT_PROTOCOL = BytesValueProtocol
      def steps(self):
        return [
            MRStep(mapper=self.mapper_get_Lecture2,
                   reducer=self.reducer_Nettoyage2) 
                   
                 ]
    
    #Lecture du fichier LivresAuteursLangues.txt
      def mapper_get_Lecture2(self, _, line):
             line=  line.replace("Langue originale : ","")
             line=  line.replace("Titre original :","")
             line = line.replace(" : " ,":")
             line = line.replace(" , " ,",")
             line = line.replace(", " ,",")    
             line = line.replace(" ," ,",") 
             livres=line.split(",")
             if len(livres)==5:
                livre=livres[0]+",Français:"+livres[1]+"#"+livres[3]+":"+" "+livres[4]+","+livres[2]
             else :
               livre=livres[0]+",Français:"+livres[1]+","+livres[2]+livres[3]
             yield None, livre
    #
      def reducer_Nettoyage2(self,_,livres):
          for livre in list(livres):
              yield _, bytes(livre, 'utf-8')
if __name__ == '__main__':
         Ev3Partie2.run()