





select * from livresev3eq1 WHERE size(auteurs) = 2;

select * from livresev3eq1 WHERE livreOriginal.langue = 'Espagnol';



select * from livresev3eq1 WHERE livreOriginal.langue IS NULL; 