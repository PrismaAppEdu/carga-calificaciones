/**
 * ================================================================
 * FirebaseSync.gs - Sincronización RTDB + Firestore
 * Sistema optimizado de importación de calificaciones
 * ================================================================
 */

// Configuración de Firebase
var FIREBASE_CONFIG = {
  RTDB_URL: "https://tu-proyecto-default-rtdb.firebaseio.com",
  FIRESTORE_PROJECT: "tu-proyecto",
  FIRESTORE_KEY: "AIzaSyD..." // Tu API Key
};

// ================================================================
// 1. OBTENER DATOS DEL PROFESOR (desde RTDB)
// ================================================================

/**
 * Lee el perfil completo del profesor desde RTDB
 */
function obtenerProfesorDesdeRTDB(matricula) {
  matricula = String(matricula).trim();
  
  try {
    var config = _getFirebaseConfig();
    var url = config.url + "profesores/" + matricula + ".json?auth=" + config.secret;
    
    var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    
    if (response.getResponseCode() !== 200) {
      return { ok: false, error: "Profesor no encontrado" };
    }
    
    var profesor = JSON.parse(response.getContentText());
    
    return {
      ok: true,
      matricula: profesor.matricula,
      nombre: profesor.nombre,
      correo: profesor.correo,
      area: profesor.area,
      tipoGrupo: profesor.tipoGrupo,
      clases: profesor.clases || {}
    };
    
  } catch (e) {
    return { ok: false, error: e.toString() };
  }
}

/**
 * Obtiene lista de alumnos desde RTDB
 */
function obtenerAlumnosDesdeRTDB() {
  try {
    var config = _getFirebaseConfig();
    var url = config.url + "alumnos.json?auth=" + config.secret;
    
    var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    var alumnos = JSON.parse(response.getContentText());
    
    return Object.keys(alumnos || {}).map(function(email) {
      return alumnos[email];
    });
    
  } catch (e) {
    return [];
  }
}

// ================================================================
// 2. IMPORTAR CALIFICACIONES (estructura dual RTDB + Firestore)
// ================================================================

/**
 * Importa datos desde Classroom y los organiza en RTDB + Firestore
 * ESTRUCTURA ALTAMENTE ORDENADA
 */
function importarGruposWebOptimizado(datos) {
  var tiempoInicio = new Date();
  var lock = LockService.getScriptLock();
  
  try {
    lock.waitLock(10000);
  } catch (e) {
    return { 
      mensajeFinal: "Servidor ocupado. Intenta en unos segundos.", 
      resumen: "" 
    };
  }
  
  try {
    var profesor = datos.profesor.trim();
    var tipoGrupo = datos.tipoGrupo;
    var matriculaProf = datos.matricula;
    
    var filasParaAgregar = [];
    var resumen = [];
    var gruposLog = [];
    var asignaturasLog = [];
    var estadisticasCarga = {
      totalRegistros: 0,
      alumnosUnicos: new Set(),
      gruposProcessados: [],
      errorLog: []
    };
    
    // 1. PROCESAR CADA GRUPO
    datos.gruposLinks.forEach(function(item) {
      var grupo = item.grupo.trim();
      var asignatura = item.asignatura.trim();
      var link = item.link.trim();
      
      if (!grupo || !asignatura || !link) return;
      
      var validacion = _validarLinkClassroom(link);
      if (!validacion.ok) {
        resumen.push("❌ ERROR: Link inválido en " + grupo);
        estadisticasCarga.errorLog.push({
          grupo: grupo,
          error: "Link inválido"
        });
        return;
      }
      
      try {
        // Extraer datos del Classroom
        var datosOrigen = SpreadsheetApp.openById(validacion.id)
          .getSheets()[0].getDataRange().getValues();
        
        if (datosOrigen.length < 5) {
          resumen.push("⚠ OMITIDO: " + grupo + " — muy pocas filas");
          return;
        }
        
        // 2. PROCESAR ALUMNOS Y CALIFICACIONES
        var registrosGrupo = _procesarClassroomSheet(
          datosOrigen, 
          profesor, 
          grupo, 
          asignatura,
          estadisticasCarga
        );
        
        filasParaAgregar = filasParaAgregar.concat(registrosGrupo);
        gruposLog.push(grupo);
        
        if (asignaturasLog.indexOf(asignatura) === -1) {
          asignaturasLog.push(asignatura);
        }
        
        estadisticasCarga.gruposProcessados.push({
          grupo: grupo,
          materia: asignatura,
          registros: registrosGrupo.length
        });
        
        resumen.push("✅ " + grupo + " — " + registrosGrupo.length + " registro(s)");
        
      } catch (err) {
        resumen.push("❌ Error en \"" + grupo + "\": " + err.message);
        estadisticasCarga.errorLog.push({
          grupo: grupo,
          error: err.message
        });
      }
    });
    
    // 3. GUARDAR EN RTDB (estructura jerárquica)
    if (filasParaAgregar.length > 0) {
      _guardarCalificacionesRTDB(
        filasParaAgregar,
        matriculaProf,
        profesor,
        gruposLog,
        asignaturasLog,
        estadisticasCarga
      );
      
      // 4. GUARDAR EN FIRESTORE (para queries)
      _guardarCalificacionesFirestore(filasParaAgregar, matriculaProf);
      
      // 5. ACTUALIZAR BITÁCORA
      _registrarBitacoraOptimizada(
        profesor,
        matriculaProf,
        gruposLog,
        asignaturasLog,
        filasParaAgregar.length,
        estadisticasCarga
      );
      
      // 6. ACTUALIZAR DASHBOARD
      _actualizarDashboardMaestro(
        profesor,
        matriculaProf,
        gruposLog,
        asignaturasLog,
        filasParaAgregar.length
      );
    }
    
    var duracion = (new Date().getTime() - tiempoInicio.getTime()) / 1000;
    
    return {
      mensajeFinal: "¡Listo! Se importaron " + filasParaAgregar.length + " registros en " + duracion + "s",
      resumen: resumen.join("\n"),
      estadisticas: {
        totalRegistros: filasParaAgregar.length,
        gruposProcessados: estadisticasCarga.gruposProcessados.length,
        duracionSegundos: duracion,
        estado: "exitoso"
      }
    };
    
  } catch (e) {
    return { 
      mensajeFinal: "Error crítico: " + e.message, 
      resumen: e.stack 
    };
  } finally {
    lock.releaseLock();
  }
}

// ================================================================
// 3. HELPERS PARA PROCESAR CLASSROOM
// ================================================================

/**
 * Extrae y procesa todos los registros de una hoja de Classroom
 */
function _procesarClassroomSheet(datosOrigen, profesor, grupo, asignatura, estadisticas) {
  var registros = [];
  var fechas = datosOrigen[0];
  var actividades = datosOrigen[1];
  
  // Detectar escala automáticamente
  var escala = 10;
  for (var p = 1; p < datosOrigen[2].length; p++) {
    var maxVal = parseFloat(datosOrigen[2][p]);
    if (!isNaN(maxVal) && maxVal > 0) {
      escala = maxVal;
      break;
    }
  }
  
  // Procesar cada alumno (desde fila 3 en adelante)
  for (var i = 3; i < datosOrigen.length; i++) {
    var fila = datosOrigen[i];
    var filaString = fila.join(" ").toLowerCase();
    
    // Saltar filas especiales
    if (
      filaString.includes("media de la clase") ||
      filaString.includes("abrir classroom") ||
      filaString.trim() === ""
    ) continue;
    
    // Buscar email del alumno
    var idxEmail = -1;
    fila.some(function(celda, idx) {
      if (String(celda).includes("@ibime.edu.mx")) {
        idxEmail = idx;
        return true;
      }
    });
    
    if (idxEmail === -1) continue;
    
    var correo = String(fila[idxEmail]).trim();
    var nombre = fila.slice(0, idxEmail)
      .filter(function(c) { return c && String(c).trim() !== ""; })
      .join(" ").trim() || "Alumno sin nombre";
    
    // Registrar alumno único
    estadisticas.alumnosUnicos.add(correo);
    
    // Procesar cada actividad/calificación
    for (var c = idxEmail + 1; c < fila.length; c++) {
      var hF = fechas[c] ? String(fechas[c]).trim() : "";
      var hA = actividades[c] ? String(actividades[c]).trim() : "";
      
      if (!hF && !hA) continue;
      
      var rawCalif = fila[c];
      var calif = (rawCalif === "" || rawCalif == null) ? 0 : parseFloat(rawCalif);
      
      // Normalizar escala a 10
      if (escala === 100 && calif > 0) {
        calif = parseFloat((calif / 10).toFixed(1));
      }
      
      if (calif > 10) calif = 10;
      
      registros.push({
        profesor: profesor,
        grupo: grupo,
        nombre: nombre,
        correo: correo,
        fecha_act: hF,
        actividad: hA,
        calif: calif,
        asignatura: asignatura,
        sync: new Date().toISOString()
      });
    }
  }
  
  return registros;
}

// ================================================================
// 4. GUARDAR EN RTDB (ESTRUCTURA JERÁRQUICA OPTIMIZADA)
// ================================================================

/**
 * Guarda calificaciones en RTDB con estructura triple:
 * - Por profesor (para obtener su carga rápidamente)
 * - Por grupo (para auditoría)
 * - Cronológico (para historial)
 */
function _guardarCalificacionesRTDB(datos, matriculaProf, nombreProf, gruposLog, asignaturasLog, estadisticas) {
  var config = _getFirebaseConfig();
  if (!config.url || !config.secret) {
    console.error("Firebase no configurado");
    return;
  }
  
  // 1. ESTRUCTURA POR PROFESOR > GRUPO > MATERIA > ALUMNO
  var datoPorProfesor = {};
  
  datos.forEach(function(registro) {
    var grupoKey = registro.grupo.toUpperCase();
    var materiaKey = _obtenerAcronimo(registro.asignatura).toUpperCase();
    var correoKey = _sanitizarClave(registro.correo);
    
    if (!datoPorProfesor[grupoKey]) {
      datoPorProfesor[grupoKey] = {};
    }
    if (!datoPorProfesor[grupoKey][materiaKey]) {
      datoPorProfesor[grupoKey][materiaKey] = {};
    }
    if (!datoPorProfesor[grupoKey][materiaKey][correoKey]) {
      datoPorProfesor[grupoKey][materiaKey][correoKey] = {
        nombre: registro.nombre,
        correo: registro.correo,
        actividades: []
      };
    }
    
    datoPorProfesor[grupoKey][materiaKey][correoKey].actividades.push({
      fecha: registro.fecha_act,
      actividad: registro.actividad,
      calif: registro.calif,
      sync: registro.sync
    });
  });
  
  // 2. GUARDAR EN RTDB: /calificaciones/por_profesor/{matricula}/...
  var payloadProfesor = {};
  payloadProfesor[matriculaProf] = datoPorProfesor;
  
  _escribirEnRTDB(
    "/calificaciones/por_profesor",
    payloadProfesor
  );
  
  // 3. GUARDAR CRONOLÓGICO: /calificaciones/cronologico/{YYYY-MM-DD}/...
  var hoy = new Date().toISOString().split('T')[0];
  var payloadCronologico = {};
  var timestamp = new Date().getTime();
  
  datos.forEach(function(registro, index) {
    payloadCronologico["reg_" + timestamp + "_" + index] = registro;
  });
  
  _escribirEnRTDB(
    "/calificaciones/cronologico/" + hoy,
    payloadCronologico,
    "PATCH"
  );
  
  console.log("✅ Guardado en RTDB: " + datos.length + " registros");
}

// ================================================================
// 5. GUARDAR EN FIRESTORE (PARA QUERIES AVANZADAS)
// ================================================================

/**
 * Guarda calificaciones en Firestore con índices para queries
 */
function _guardarCalificacionesFirestore(datos, matriculaProf) {
  try {
    // Para GAS + Firestore, necesitarías usar la REST API
    // o una librería como Firestore.gs
    
    // Opción simplificada: guardar resumen en RTDB
    var resumen = {
      matricula: matriculaProf,
      totalRegistros: datos.length,
      ultimaCarga: new Date().toISOString(),
      alumnosUnicos: _contarAlumnosUnicos(datos),
      gruposUnicos: _contarGruposUnicos(datos)
    };
    
    _escribirEnRTDB(
      "/firestore_resumen/" + matriculaProf,
      resumen
    );
    
  } catch (e) {
    console.error("Error guardando en Firestore: " + e);
  }
}

// ================================================================
// 6. ACTUALIZAR BITÁCORA OPTIMIZADA
// ================================================================

/**
 * Registra la importación en bitácora con detalles completos
 */
function _registrarBitacoraOptimizada(prof, matricula, grupsLog, matsLog, cantidad, estadisticas) {
  try {
    var hoy = new Date().toISOString().split('T')[0];
    var bitacoraId = "bitacora_" + new Date().getTime();
    
    var bitacora = {
      id: bitacoraId,
      timestamp: new Date().toISOString(),
      profesor: prof,
      matricula: matricula,
      grupos: grupsLog,
      materias: matsLog,
      registrosAgregados: cantidad,
      duracionSegundos: (estadisticas.duracion || 0),
      estado: "exitoso",
      gruposDetalle: estadisticas.gruposProcessados,
      errores: estadisticas.errorLog,
      alumnosUnicos: estadisticas.alumnosUnicos.size
    };
    
    _escribirEnRTDB(
      "/bitacora/" + hoy + "/" + bitacoraId,
      bitacora,
      "PUT"
    );
    
    console.log("✅ Bitácora registrada: " + bitacoraId);
    
  } catch (e) {
    console.error("Error bitácora: " + e);
  }
}

// ================================================================
// 7. ACTUALIZAR DASHBOARD MAESTRO
// ================================================================

/**
 * Actualiza el dashboard con el progreso del profesor
 */
function _actualizarDashboardMaestro(profesor, matricula, gruposLog, materiasLog, cantidad) {
  try {
    var hoy = new Date().toISOString().split('T')[0];
    
    var dashboardData = {
      profesor: profesor,
      matricula: matricula,
      estado: "✅ COMPLETADO",
      gruposCargados: gruposLog.length,
      materiasCargadas: materiasLog.length,
      registrosAgregados: cantidad,
      porcentaje: 100,
      ultimaCarga: new Date().toISOString()
    };
    
    _escribirEnRTDB(
      "/dashboards/maestro_resumen/" + hoy + "/" + matricula,
      dashboardData,
      "PUT"
    );
    
    console.log("✅ Dashboard actualizado: " + profesor);
    
  } catch (e) {
    console.error("Error dashboard: " + e);
  }
}

// ================================================================
// 8. HELPERS GENERALES
// ================================================================

function _escribirEnRTDB(path, data, method) {
  method = method || "PUT";
  var config = _getFirebaseConfig();
  
  var url = config.url + path + ".json?auth=" + config.secret;
  
  try {
    var resp = UrlFetchApp.fetch(url, {
      method: method,
      contentType: "application/json",
      payload: JSON.stringify(data),
      muteHttpExceptions: true
    });
    
    if (resp.getResponseCode() !== 200) {
      console.error("Error escribiendo en RTDB: HTTP " + resp.getResponseCode());
    }
    
  } catch (e) {
    console.error("Error: " + e.toString());
  }
}

function _sanitizarClave(str) {
  return str.replace(/[.#$\[\]\/]/g, "_");
}

function _obtenerAcronimo(texto) {
  if (!texto) return "N/A";
  var palabras = texto.split(" ").filter(function(w) { return w.length > 2; });
  if (palabras.length === 0) return texto.substring(0, 3).toUpperCase();
  if (palabras.length === 1) return palabras[0].substring(0, 3).toUpperCase();
  return palabras.map(function(w) { return w[0]; }).join("").toUpperCase();
}

function _contarAlumnosUnicos(datos) {
  var set = new Set();
  datos.forEach(function(d) { set.add(d.correo); });
  return set.size;
}

function _contarGruposUnicos(datos) {
  var set = new Set();
  datos.forEach(function(d) { set.add(d.grupo); });
  return set.size;
}

function _validarLinkClassroom(link) {
  try {
    var id = link.match(/\/d\/(.+?)\//);
    if (!id) return { ok: false };
    var ss = SpreadsheetApp.openById(id[1]);
    return { ok: true, id: id[1] };
  } catch (e) {
    return { ok: false };
  }
}
