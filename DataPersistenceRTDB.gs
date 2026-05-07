/**
 * ================================================================
 * DataPersistenceRTDB.gs — Guarda datos en la RTDB DESTINO
 * 
 * PROPÓSITO: Guardar calificaciones cargadas por docentes
 * en estructura clara y accesible para consultas
 * 
 * USO: DataPersistence.saveCalificaciones(), updateCalificaciones(), etc.
 * ================================================================
 */

var DataPersistenceRTDB = {
  
  // ════════════════════════════════════════════════════════════════
  // 1. GUARDAR CALIFICACIONES (INSERT)
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Guarda un registro individual de calificación
   * Estructura: calificaciones > {id} > { profesor, alumno, grupo, materia, calif, fecha }
   */
  saveCalificacion: function(calificacionData) {
    try {
      var config = _getFirebaseConfig();
      var calificacionId = Utilities.getUuid();
      
      var dataToSave = {
        id: calificacionId,
        profesor: calificacionData.profesor || "",
        matriculaProfesor: calificacionData.matriculaProfesor || "",
        alumno: calificacionData.alumno || "",
        correoAlumno: calificacionData.correoAlumno || "",
        grupo: calificacionData.grupo || "",
        materia: calificacionData.materia || "",
        calificacion: parseFloat(calificacionData.calificacion) || 0,
        fecha_actividad: calificacionData.fecha_actividad || "",
        actividad: calificacionData.actividad || "",
        fecha_carga: new Date().toISOString(),
        estado: "procesado"
      };
      
      var url = config.url + "calificaciones/" + calificacionId + ".json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, {
        method: "PUT",
        contentType: "application/json",
        payload: JSON.stringify(dataToSave),
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() === 200) {
        console.log("✅ Calificación guardada: " + calificacionId);
        return { ok: true, id: calificacionId };
      } else {
        console.error("Error guardando calificación. HTTP: " + response.getResponseCode());
        return { ok: false, error: "Error HTTP " + response.getResponseCode() };
      }
      
    } catch (e) {
      console.error("❌ Error en saveCalificacion: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  /**
   * Guarda múltiples calificaciones en un lote (más eficiente)
   */
  saveCalificacionesLote: function(calificacionesArray) {
    try {
      var config = _getFirebaseConfig();
      var updates = {};
      var ids = [];
      
      calificacionesArray.forEach(function(calif) {
        var id = Utilities.getUuid();
        ids.push(id);
        
        updates[id] = {
          id: id,
          profesor: calif.profesor || "",
          matriculaProfesor: calif.matriculaProfesor || "",
          alumno: calif.alumno || "",
          correoAlumno: calif.correoAlumno || "",
          grupo: calif.grupo || "",
          materia: calif.materia || "",
          calificacion: parseFloat(calif.calificacion) || 0,
          fecha_actividad: calif.fecha_actividad || "",
          actividad: calif.actividad || "",
          fecha_carga: new Date().toISOString(),
          estado: "procesado"
        };
      });
      
      var url = config.url + "calificaciones.json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, {
        method: "PATCH",
        contentType: "application/json",
        payload: JSON.stringify(updates),
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() === 200) {
        console.log("✅ Lote guardado: " + ids.length + " registros");
        return { ok: true, cantidad: ids.length, ids: ids };
      } else {
        console.error("Error guardando lote. HTTP: " + response.getResponseCode());
        return { ok: false, error: "Error HTTP " + response.getResponseCode() };
      }
      
    } catch (e) {
      console.error("❌ Error en saveCalificacionesLote: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 2. ACTUALIZAR CALIFICACIONES (UPDATE)
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Actualiza una calificación existente
   */
  updateCalificacion: function(calificacionId, datosActualizacion) {
    try {
      var config = _getFirebaseConfig();
      
      var url = config.url + "calificaciones/" + calificacionId + ".json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, {
        method: "PATCH",
        contentType: "application/json",
        payload: JSON.stringify({
          ...datosActualizacion,
          fecha_actualizacion: new Date().toISOString()
        }),
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() === 200) {
        console.log("✅ Calificación actualizada: " + calificacionId);
        return { ok: true };
      } else {
        return { ok: false, error: "Error HTTP " + response.getResponseCode() };
      }
      
    } catch (e) {
      console.error("❌ Error en updateCalificacion: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 3. GUARDAR POR ESTRUCTURA (Profesor > Grupo > Materia > Alumno)
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Guarda calificaciones en estructura jerárquica para consultas rápidas
   * Ruta: /calificaciones_por_profesor/{matricula}/{grupo}/{materia}/{alumno}
   */
  saveCalificacionesEstructurado: function(matriculaProfesor, datosEstructurados) {
    try {
      var config = _getFirebaseConfig();
      var hoy = new Date().toISOString().split('T')[0];
      
      var url = config.url + "calificaciones_por_profesor/" + matriculaProfesor + "/" + hoy + ".json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, {
        method: "PATCH",
        contentType: "application/json",
        payload: JSON.stringify(datosEstructurados),
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() === 200) {
        console.log("✅ Datos estructurados guardados para: " + matriculaProfesor);
        return { ok: true };
      } else {
        return { ok: false, error: "Error HTTP " + response.getResponseCode() };
      }
      
    } catch (e) {
      console.error("❌ Error en saveCalificacionesEstructurado: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 4. GUARDAR EN ÍNDICES (para búsquedas rápidas)
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Guarda índices de calificaciones por:
   * - Alumno (correo)
   * - Grupo
   * - Profesor
   */
  saveIndices: function(calificacion) {
    try {
      var config = _getFirebaseConfig();
      var timestamp = new Date().getTime();
      
      // Índice por alumno
      var indiceAlumno = {
        profesor: calificacion.profesor,
        grupo: calificacion.grupo,
        materia: calificacion.materia,
        calificacion: calificacion.calificacion,
        fecha: new Date().toISOString()
      };
      
      // Índice por grupo
      var indiceGrupo = {
        profesor: calificacion.profesor,
        alumno: calificacion.alumno,
        materia: calificacion.materia,
        calificacion: calificacion.calificacion,
        fecha: new Date().toISOString()
      };
      
      var urlAlumno = config.url + "indices_por_alumno/" + _sanitizarClave(calificacion.correoAlumno) + ".json?auth=" + config.secret;
      var urlGrupo = config.url + "indices_por_grupo/" + calificacion.grupo + ".json?auth=" + config.secret;
      
      UrlFetchApp.fetch(urlAlumno, {
        method: "PATCH",
        contentType: "application/json",
        payload: JSON.stringify(indiceAlumno),
        muteHttpExceptions: true
      });
      
      UrlFetchApp.fetch(urlGrupo, {
        method: "PATCH",
        contentType: "application/json",
        payload: JSON.stringify(indiceGrupo),
        muteHttpExceptions: true
      });
      
      return { ok: true };
      
    } catch (e) {
      console.error("❌ Error en saveIndices: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 5. GUARDAR METADATOS DE LA CARGA
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Registra metadatos sobre la carga de calificaciones
   */
  saveMetaCarga: function(matriculaProfesor, nombreProfesor, estadisticas) {
    try {
      var config = _getFirebaseConfig();
      var hoy = new Date().toISOString().split('T')[0];
      var timestamp = new Date().getTime();
      
      var metaData = {
        id: "meta_" + timestamp,
        profesor: nombreProfesor,
        matricula: matriculaProfesor,
        fecha: new Date().toISOString(),
        grupos: estadisticas.grupos || [],
        materias: estadisticas.materias || [],
        totalRegistros: estadisticas.totalRegistros || 0,
        totalAlumnos: estadisticas.totalAlumnos || 0,
        estado: "completado"
      };
      
      var url = config.url + "meta_cargas/" + hoy + "/meta_" + timestamp + ".json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, {
        method: "PUT",
        contentType: "application/json",
        payload: JSON.stringify(metaData),
        muteHttpExceptions: true
      });
      
      if (response.getResponseCode() === 200) {
        console.log("✅ Metadatos guardados");
        return { ok: true };
      }
      
    } catch (e) {
      console.error("❌ Error en saveMetaCarga: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  }
  
};
