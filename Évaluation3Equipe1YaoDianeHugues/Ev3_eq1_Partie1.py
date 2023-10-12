""" Ce programme permet la lecture et le nettoyage du fichier LivresAuteursLangues.txt"""

 #|OUTPUT_PROTOCOL = BytesValueProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
#from LangueOriginale import LangueOriginaleFct
from mrjob.protocol import BytesValueProtocol

class Ev3Partie1(MRJob):

      OUTPUT_PROTOCOL = BytesValueProtocol

      def steps(self):

        return [

            MRStep(mapper=self.mapper_get_Lecture1,

                   reducer=self.reducer_Nettoyage1)

            ,

              MRStep( reducer=self.reducer_Numerotation1 )          

                 ]

   

    #Lecture du fichier LivresAuteursLangues.txt

      def mapper_get_Lecture1(self, _, line):
             line = line.replace("-",",")

             line = line.replace(", Auteur inconnu (vous pouvez préciser l'auteur si vous le connaissez)" ,"")  

             line = line.replace("Titre original : Inconnu","")  

             line = line.replace("Langue originale : Inconnue","")

             line = line.replace("Langue originale : Français , Titre original : Le Mystère de la Chambre Jaune","")
             line = line.replace("Langue originale : Français , Titre original : L'Étranger","")
             line = line.replace("Langue originale : Français , Titre original : Le Petit Prince","")
             line = line.replace("Langue originale : Français , Titre original : Les Misérables","")
             line = line.replace("Langue originale : Français , Titre original : L'Écume des jours","")
             line = line.replace("Langue originale : Français , Titre original : La Peste","")
             line = line.replace("Langue originale : Français , Titre original : L'Assommoir","")
             line = line.replace("Langue originale : Français , Titre original : Le Comte de Monte-Cristo","")
             line = line.replace("Langue originale : Français , Titre original : Le Rouge et le Noir","")
             line = line.replace("Langue originale : Français , Titre original : Les Trois Mousquetaires","")
             line = line.replace("Langue originale : Français , Titre original : L'Éducation sentimentale","")
             line = line.replace("Langue originale : Français , Titre original : Les Fourmis","")
             line = line.replace("Langue originale : Français , Titre original : La Nuit des temps","")
             line = line.replace("Langue originale : Français , Titre original : La Quête d'Ewilan","")
             line = line.replace("Langue originale : Français , Titre original : Les Dieux ont soif","")
             line = line.replace("Langue originale : Français , Titre original : La Trilogie de la Nuit","")
             line = line.replace("Langue originale : Français , Titre original : La Nuit des temps","")
             line = line.replace("Langue originale : Français , Titre original : La Trilogie du Secret de Ji","")
             line = line.replace("Langue originale : Français , Titre original : La Trilogie des Elfes","")
             line = line.replace("Langue originale : Français , Titre original : La Trilogie de l'Éveil","")
             line = line.replace("Langue originale : Français , Titre original : La Trilogie des Dieux","")
             line = line.replace("Langue originale : Français , Titre original : Le Cycle des Immortels","")
             line = line.replace("Langue originale : Français , Titre original : La Trilogie de la Nuit","")
             line = line.replace("Langue originale : Français , Titre original : La Trilogie des Dieux","")
             line = line.replace("Langue originale : Français , Titre original : Le Cycle des Immortels","")
             line = line.replace("Langue originale : Français , Titre original : La Trilogie du Secret de Ji","")
             line = line.replace("Langue originale : Français , Titre original : La Nuit des temps","")
             #line = line.replace("Titre original : Inconnu"," ")

             #if LangueOriginaleFct(line) == "Langue originale : Français" :

                #line = line[0:line.index("-")-1] + " ,"

             livres=line.split('\n')

                     

             yield None, line

           

             

     

      #def   reducer_Numerotation1 (self,_,livres):

       # for livre in list(livres):

         #yield _, bytes(str(livre), 'utf-8')

         

      def reducer_Nettoyage1(self,_,livres):

        i=1

        for livre in livres:

           yield _, str(i)+","+ livre

           i += 1

           

      def   reducer_Numerotation1 (self,_,livres):

        for livre in list(livres):

         yield _, bytes(str(livre), 'utf-8')    

         

       

if __name__ == '__main__':

         Ev3Partie1.run()