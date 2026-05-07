/**
 * ================================================================
 * DataReaderRTDB.gs — Lee datos de la RTDB FUENTE
 * 
 * PROPÓSITO: Obtener profesores, alumnos, grupos, asignaturas
 * desde el primer Firebase (datos maestros)
 * 
 * USO: DataReader.getProfesores(), DataReader.getAlumnos(), etc.
 * ================================================================
 */

var DataReaderRTDB = {
  
  // ════════════════════════════════════════════════════════════════
  // 1. OBTENER PROFESORES DESDE RTDB
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Lee todos los profesores del RTDB fuente
   * Retorna un array con estructura: { matricula, nombre, correo, area, tipo }
   */
  getProfesores: function() {
    try {
      var config = _getFirebaseConfig();
      var url = config.url + "profesores.json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        console.error("Error leyendo profesores. HTTP: " + response.getResponseCode());
        return [];
      }
      
      var datos = JSON.parse(response.getContentText());
      var profesores = [];
      
      for (var key in datos) {
        if (datos.hasOwnProperty(key)) {
          profesores.push(datos[key]);
        }
      }
      
      console.log("✅ Profesores cargados: " + profesores.length);
      return profesores;
      
    } catch (e) {
      console.error("❌ Error en getProfesores: " + e.toString());
      return [];
    }
  },
  
  /**
   * Obtiene un profesor específico por su matrícula
   */
  getProfesorByMatricula: function(matricula) {
    try {
      var config = _getFirebaseConfig();
      var url = config.url + "profesores/" + matricula + ".json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() === 200) {
        return JSON.parse(response.getContentText());
      }
      return null;
      
    } catch (e) {
      console.error("Error en getProfesorByMatricula: " + e.toString());
      return null;
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 2. OBTENER ALUMNOS DESDE RTDB
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Lee todos los alumnos del RTDB fuente
   * Retorna array con: { matricula, nombre, tutor, grupoEsp, grupoIng, correo }
   */
  getAlumnos: function() {
    try {
      var config = _getFirebaseConfig();
      var url = config.url + "alumnos.json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        console.error("Error leyendo alumnos. HTTP: " + response.getResponseCode());
        return [];
      }
      
      var datos = JSON.parse(response.getContentText());
      var alumnos = [];
      
      for (var key in datos) {
        if (datos.hasOwnProperty(key)) {
          alumnos.push(datos[key]);
        }
      }
      
      console.log("✅ Alumnos cargados: " + alumnos.length);
      return alumnos;
      
    } catch (e) {
      console.error("❌ Error en getAlumnos: " + e.toString());
      return [];
    }
  },
  
  /**
   * Obtiene un alumno por su correo
   */
  getAlumnoByCorreo: function(correo) {
    try {
      var config = _getFirebaseConfig();
      var correoSanitized = _sanitizarClave(correo);
      var url = config.url + "alumnos/" + correoSanitized + ".json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() === 200) {
        return JSON.parse(response.getContentText());
      }
      return null;
      
    } catch (e) {
      console.error("Error en getAlumnoByCorreo: " + e.toString());
      return null;
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 3. OBTENER GRUPOS
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Lee todos los grupos (español e inglés)
   */
  getGrupos: function() {
    try {
      var config = _getFirebaseConfig();
      var url = config.url + "grupos.json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        console.error("Error leyendo grupos. HTTP: " + response.getResponseCode());
        return [];
      }
      
      var datos = JSON.parse(response.getContentText());
      var grupos = [];
      
      for (var key in datos) {
        if (datos.hasOwnProperty(key)) {
          grupos.push(datos[key]);
        }
      }
      
      console.log("✅ Grupos cargados: " + grupos.length);
      return grupos;
      
    } catch (e) {
      console.error("❌ Error en getGrupos: " + e.toString());
      return [];
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 4. OBTENER ASIGNATURAS
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Lee todas las asignaturas disponibles
   */
  getAsignaturas: function() {
    try {
      var config = _getFirebaseConfig();
      var url = config.url + "asignaturas.json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        console.error("Error leyendo asignaturas. HTTP: " + response.getResponseCode());
        return [];
      }
      
      var datos = JSON.parse(response.getContentText());
      var asignaturas = [];
      
      for (var key in datos) {
        if (datos.hasOwnProperty(key)) {
          asignaturas.push(datos[key]);
        }
      }
      
      console.log("✅ Asignaturas cargadas: " + asignaturas.length);
      return asignaturas;
      
    } catch (e) {
      console.error("❌ Error en getAsignaturas: " + e.toString());
      return [];
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 5. OBTENER DATOS COMBINADOS PARA UN PROFESOR
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Obtiene TODA la información que necesita un profesor
   * (su perfil, sus grupos, sus alumnos, asignaturas)
   */
  getDataCompletoProfesor: function(matriculaProfesor) {
    try {
      var profesor = DataReaderRTDB.getProfesorByMatricula(matriculaProfesor);
      
      if (!profesor) {
        return { ok: false, error: "Profesor no encontrado" };
      }
      
      var grupos = DataReaderRTDB.getGrupos();
      var alumnos = DataReaderRTDB.getAlumnos();
      var asignaturas = DataReaderRTDB.getAsignaturas();
      
      // Filtrar grupos del profesor
      var gruposProfesor = grupos.filter(function(g) {
        return g.profesorId === matriculaProfesor;
      });
      
      // Filtrar alumnos de sus grupos
      var alumnosProfesor = alumnos.filter(function(a) {
        return gruposProfesor.some(function(g) {
          return g.id === a.grupoId;
        });
      });
      
      return {
        ok: true,
        profesor: profesor,
        grupos: gruposProfesor,
        alumnos: alumnosProfesor,
        asignaturas: asignaturas
      };
      
    } catch (e) {
      console.error("❌ Error en getDataCompletoProfesor: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  }
  
};
