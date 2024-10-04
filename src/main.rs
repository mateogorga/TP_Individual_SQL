use std::env;
use std::fmt::Error;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::fs::OpenOptions;
use std::fs;

enum ErrorType {
    InvalidTable,
    InvalidColumn,
    InvalidSintax,
    Error,
}

fn mostrar_error(tipo: ErrorType, descripcion: &str) {

    match tipo {
        ErrorType::InvalidColumn => println!("[INVALID_COLUMN]: {}", descripcion),
        ErrorType::InvalidTable => println!("[INVALID_TABLE]: {}", descripcion),
        ErrorType::InvalidSintax => println!("[INVALID_SINTAX]: {}", descripcion),
        ErrorType::Error => println!("[ERROR]: {}", descripcion),
    }
}

fn generar_ruta(ruta: &str, nombre_tabla: &str) -> String{
    ruta.to_string() + "/" + nombre_tabla + ".csv"
}

fn convertir_a_entero(num: &String) -> i32{
    let entero: i32 = match num.parse::<i32>() {
        Ok(valor) => valor,
        Err(_) => {
            mostrar_error(ErrorType::Error, "Error al convertir el entero");            0 // Valor predeterminado
        },
    };

    entero
}

fn crear_archivo() -> Result<File, ()> {

    let path_new = PathBuf::from("archivo_temporal.csv");
    let file_new = match OpenOptions::new().write(true).create(true).truncate(true).open(&path_new) {
        Ok(f) => f,
        Err(e) => {
            mostrar_error(ErrorType::Error, "Error al crear el archivo temporal.");
            return Err(());
        },
    };
    Ok(file_new)
}

fn agregar_lineas(writer: &mut BufWriter<File>, nombres_columnas: &String, reader: &mut BufReader<File>) {
    let lineas = reader.lines();

    if writeln!(writer, "{}", nombres_columnas).is_err() {
        mostrar_error(ErrorType::Error, "Error al escribir el archivo");
        return;
    }

    for linea in lineas {
        match linea {
            Ok(l) => {
                if writeln!(writer, "{}", l).is_err() {
                    mostrar_error(ErrorType::Error, "Error al escribir el archivo");
                    return;
                }
            }
            Err(e) => {
                mostrar_error(ErrorType::Error, "Error al leer el archivo");
                return;
            }
        }
    }
}

fn obtener_nombres_columnas(reader: &mut BufReader<File>) -> String {
    let mut lineas = reader.lines();

    let primera_linea = match lineas.next() {
        Some(Ok(line)) => line,
        Some(Err(_e)) => {
            mostrar_error(ErrorType::InvalidColumn, "Error al leer el nombre de las columnas");
            return ' '.to_string();
        },
        None => {mostrar_error(ErrorType::InvalidTable, "El archivo esta vacio.");
                return ' '.to_string();},
    };

    primera_linea
}

fn agrupar_valores(consulta: &Vec<String>) -> Vec<&String> {
    let mut valores = Vec::new();
    let n = consulta.len();
    let mut contador = 4;

    if n == 6 {
        valores.push(&consulta[5])
    } else {
        while n != contador {
            valores.push(&consulta[contador+1]);
            contador += 2;
        }
    }

    valores
}

fn insert_into(consulta: Vec<String>, ruta: &str) {
    let mut cont_filas = 5;
    //Abro el archivo que contiene a la tabla
    let ruta_completa = generar_ruta(ruta, consulta[2].as_str());

    let f = match File::open(ruta_completa) {
        Ok(f) => f,
        Err(e) => {
            mostrar_error(ErrorType::InvalidTable, &format!("Error al leer la tabla: {}", e));
            return;
        },
    };

    //Creo un buffer para leer el archivo 
    let mut reader = BufReader::new(f);

    //Obtengo el nombre de las columnas
    let nombres_columnas: String = obtener_nombres_columnas(&mut reader);

    //Creo un archivo temporal donde agregaré las nuevas filas
    let file_new: Result<File, ()> = crear_archivo();

    //Agrego las lineas que ya estaban
    let file = match file_new {
        Ok(f) => f,
        Err(_) => return (),
    }; 
    let mut writer = BufWriter::new(file);
    agregar_lineas(&mut writer, &nombres_columnas, &mut reader);

    //Agrego los nuevos valores
    let columnas_nuevas: Vec<&str> = consulta[3].trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect();
    let columnas_totales: Vec<&str> = nombres_columnas.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 

    while cont_filas < consulta.len() {
        let consulta_fila = consulta[cont_filas].trim_matches(|c| c == '(' || c == ')').replace("'", "");
        let mut contenido = consulta_fila.split(',').map(|s| s.trim()).collect::<Vec<&str>>();
        let mut i = 0;

        for col in &columnas_totales {
            if !columnas_nuevas.contains(&col) {
                contenido.insert(i, "null");
            }
            i+=1;
        }

        //i = 0;
        //Agrego la nueva fila al archivo temporal
        if writeln!(writer, "{}", contenido.join(",")).is_err() {
            mostrar_error(ErrorType::Error, "Error al escribir el archivo");
            return;
        }

        cont_filas+=2;
    }

    //Cambio el nombre del archivo temporal
    let nombre_csv = consulta[2].to_string() + ".csv";
    let _ = fs::rename("archivo_temporal.csv", nombre_csv);

}

/// Recibe la consulta ingresada por el usuario y devuelve un vector de strings. 
/// Dentro de los parentesis agrupa cadenas y
/// fuera de ellos, separa por comas y espacios.
pub fn dividir_consulta(consulta: &str) -> Vec<String> { 
    let mut nueva_lista: Vec<String> = Vec::new(); 
    let mut acumulador = String::new();
    let mut en_parentesis = false;

    for c in consulta.chars() {
        match c {
            '(' => {
                if !acumulador.is_empty() {
                    nueva_lista.push(acumulador.trim().to_string()); 
                    acumulador.clear();
                }
                en_parentesis = true;
                acumulador.push(c);
            }
            ')' => {
                acumulador.push(c);
                nueva_lista.push(acumulador.trim().to_string()); 
                acumulador.clear();
                en_parentesis = false;
            }
            ',' => {
                if en_parentesis {
                    acumulador.push(c);
                } else {
                    if !acumulador.is_empty() {
                        nueva_lista.push(acumulador.trim().to_string()); 
                        acumulador.clear();
                    } else {
                        acumulador.push(c);
                    }
                }
            }
            ' ' => {
                if !en_parentesis && !acumulador.is_empty() {
                    nueva_lista.push(acumulador.trim().to_string()); 
                    acumulador.clear();
                } else {
                    acumulador.push(c);
                }
            }
            ';' => {
                if !acumulador.is_empty() {
                    nueva_lista.push(acumulador.trim().to_string()); 
                }
            }
            _ => acumulador.push(c),
        }
    }
    println!("La lista es: {:?}", nueva_lista);
    nueva_lista
}


fn select_where(consulta: Vec<String>, ruta: &str) {
    let componentes = consulta.len();

    //Abro el archivo que contiene a la tabla
    let ruta_completa = generar_ruta(ruta, consulta[3].as_str());

    let f = match File::open(ruta_completa) {
        Ok(f) => f,
        Err(e) => {
            mostrar_error(ErrorType::InvalidTable, "Error al leer la tabla");
            return;
        },
    };

    //Creo un buffer para leer el archivo 
    let mut reader = BufReader::new(f);

    //Obtengo el nombre de las columnas
    let nombres_columnas: String = obtener_nombres_columnas(&mut reader);
    let columnas: Vec<&str> = nombres_columnas.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 

    //Creo la salida de la funcion
    let mut salida = io::stdout();

    for linea in reader.lines() {
        match linea {
            Ok(line) => {
                if componentes == 8 {
                    if consulta[6] == ">" {
                        let mut i = 0;
                        while i <= columnas.len()-1 {
                            if columnas[i] == consulta[5] {
                                let lista_linea: Vec<&str> = line.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 
                                let entero_consulta: i32 = convertir_a_entero(&consulta[7]);
                                let entero_linea: i32 = convertir_a_entero(&lista_linea[i].to_string());
    
                                if entero_linea > entero_consulta {
                                    if writeln!(salida, "{}", line).is_err() {
                                        mostrar_error(ErrorType::Error, "Error al escribir el archivo");
                                        return;
                                    }
                                }                              
                            } 
                            i+=1;
                        }
                        
                    } else if consulta[6] == "<" {
                        let mut i = 0;
                        while i <= columnas.len()-1 {
                            if columnas[i] == consulta[5] {
                                let lista_linea: Vec<&str> = line.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 
                                let entero_consulta: i32 = convertir_a_entero(&consulta[7]);
                                let entero_linea: i32 = convertir_a_entero(&lista_linea[i].to_string());
    
                                if entero_linea < entero_consulta {
                                    if writeln!(salida, "{}", line).is_err() {
                                        mostrar_error(ErrorType::Error, "Error al escribir el archivo");
                                        return;
                                    }
                                }                              
                            }
                            i+=1;
                        }
                    } else if consulta[6] == "=" {
                        let mut i = 0;
                        while i <= columnas.len()-1 {
                            if columnas[i] == consulta[5] {
                                let lista_linea: Vec<&str> = line.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 
                                let entero_consulta: i32 = convertir_a_entero(&consulta[7]);
                                let entero_linea: i32 = convertir_a_entero(&lista_linea[i].to_string());
    
                                if entero_linea == entero_consulta {
                                    if writeln!(salida, "{}", line).is_err() {
                                        mostrar_error(ErrorType::Error, "Error al escribir el archivo");
                                        return;
                                    }
                                }                              
                            }
                            i+=1;
                        }
                    }
                } else {

                }      
            }
            Err(e) => {
                mostrar_error(ErrorType::InvalidTable, "Error al leer la linea");
                return;
            }
        }
    }
}

fn select_order_by() {
    //FALTA IMPLEMENTAR
}

fn select(consulta: Vec<String>, ruta: &str) {
    let comp_where = "WHERE".to_string();
    let comp_order_by = "ORDER".to_string();

    match &consulta[4] {
        comp_where => select_where(consulta, ruta),
        comp_order_by => select_order_by(),
        _ => mostrar_error(ErrorType::InvalidSintax, "Error al procesar la consulta"),
    }
}


fn update(consulta: Vec<String>, ruta: &str) {
    // Abro el archivo que contiene a la tabla
    let ruta_completa = generar_ruta(ruta, &consulta[1]);

    let f = match File::open(&ruta_completa) {
        Ok(f) => f,
        Err(e) => {
            mostrar_error(ErrorType::InvalidTable, &format!("Error al leer la tabla: {}", e));
            return;
        },
    };

    // Creo un buffer para leer el archivo 
    let mut reader = BufReader::new(f);

    // Obtengo el nombre de las columnas
    let nombres_columnas: String = obtener_nombres_columnas(&mut reader);

    // Creo un archivo temporal donde agregaré las nuevas filas
    let path_new = PathBuf::from("archivo_temporal.csv");
    let file_new = match OpenOptions::new().write(true).create(true).truncate(true).open(&path_new) {
        Ok(f) => f,
        Err(e) => {
            mostrar_error(ErrorType::Error, "Error al crear el archivo temporal.");
            return;
        },
    };

    // Agrego las líneas que ya estaban
    let mut writer = BufWriter::new(file_new);

    // Agrego la primera línea
    if writeln!(writer, "{}", nombres_columnas).is_err() {
        mostrar_error(ErrorType::Error, "Error al escribir el archivo");
        return;
    }

    // Determino la posición de la columna a actualizar
    let long = consulta.len();
    let columnas_totales: Vec<&str> = nombres_columnas.trim_matches(|c| c == '(' || c == ')').split(',').map(|s| s.trim()).collect();

    let num_cols = columnas_totales.len();
    let mut posicion = 0;

    for i in 0..columnas_totales.len() {
        if columnas_totales[i] == consulta[long - 3] {
            posicion = i;
            break;
        }
    }

    // Itero sobre las líneas del archivo original
    for linea in reader.lines() {
        match linea {
            Ok(l) => {
                let elementos: Vec<&str> = l.split_whitespace().collect();
                let consulta_valor_indice = &consulta[long - 1];
                let consulta_col_cambiar = &consulta[3];

                if elementos.get(posicion) == Some(&consulta_valor_indice.as_str()) {
                    let mut nueva_linea: Vec<&str> = Vec::with_capacity(num_cols);

                    for i in 0..num_cols {
                        if columnas_totales[i] == consulta_col_cambiar {
                            nueva_linea.push(&consulta[5]);
                        } else {
                            if let Some(elem) = elementos.get(i) {
                                nueva_linea.push(elem);
                            }
                        }
                    }

                    let nueva_linea_str = nueva_linea.join(" ");
                    if writeln!(writer, "{}", nueva_linea_str).is_err() {
                        mostrar_error(ErrorType::Error, "Error al escribir el archivo");
                        return;
                    }
                } else {
                    if writeln!(writer, "{}", l).is_err() {
                        mostrar_error(ErrorType::Error, "Error al escribir el archivo");
                        return;
                    }
                }
            }
            Err(e) => {
                mostrar_error(ErrorType::Error, "Error al leer el archivo");
                return;
            }
        }
    }

    //Cambio el nombre del archivo temporal
    let nombre_csv = consulta[1].to_string() + ".csv";
    let _ = fs::rename("archivo_temporal.csv", nombre_csv);
}

fn delete() {
    //FALTA IMPLEMENTAR
}



fn main() {

    //Obtengo un vector con los ingresos
    let ingresos : Vec<String> = env::args().collect();
    let ruta_tablas = &ingresos[1];
    let lista_consulta = dividir_consulta(&ingresos[2]); 

    match lista_consulta[0].to_uppercase().as_str() {
        "" => mostrar_error(ErrorType::InvalidSintax, "La consulta esta vacia"),
        "INSERT" => insert_into(lista_consulta, ruta_tablas),
        "SELECT" => select(lista_consulta, ruta_tablas),
        "UPDATE" => update(lista_consulta, ruta_tablas),
        "DELETE" => delete(),
        _ => mostrar_error(ErrorType::InvalidSintax, "Error en el ingreso de la consulta."),
    }
}

