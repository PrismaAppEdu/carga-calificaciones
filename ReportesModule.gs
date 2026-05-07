/**
 * ================================================================
 * ReportesModule.gs — MÓDULO DE REPORTES PARA PROFESORES
 * 
 * PROPÓSITO: Generar reportes desde Firebase para consulta de profesores
 * Reportes: calificaciones por alumno, por grupo, por materia, etc.
 * 
 * USO: Reportes.getReportesProfesor(), getCalificacionesAlumno(), etc.
 * ================================================================
 */

var Reportes = {
  
  // ════════════════════════════════════════════════════════════════
  // 1. OBTENER REPORTE COMPLETO DE UN PROFESOR
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Obtiene TODAS las calificaciones cargadas por un profesor
   * Organizado por: grupos > materias > alumnos
   */
  getReporteCompletoProfesor: function(matriculaProfesor) {
    console.log("📊 Generando reporte completo para profesor: " + matriculaProfesor);
    
    try {
      var config = _getFirebaseConfig();
      var hoy = new Date().toISOString().split('T')[0];
      
      // Leer toda la estructura del profesor
      var url = config.url + "calificaciones_por_profesor/" + matriculaProfesor + ".json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        return { ok: false, error: "Sin datos para este profesor" };
      }
      
      var datosProfesor = JSON.parse(response.getContentText());
      
      // Procesar y calcular estadísticas
      var resumenGrupos = Reportes._procesarDatos(datosProfesor);
      
      return {
        ok: true,
        profesor: matriculaProfesor,
        fecha_reporte: new Date().toISOString(),
        grupos: resumenGrupos,
        totalAlumnos: Reportes._contarAlumnosUnicos(datosProfesor),
        totalCalificaciones: Reportes._contarCalificacionesTotales(datosProfesor)
      };
      
    } catch (e) {
      console.error("❌ Error en getReporteCompletoProfesor: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 2. OBTENER REPORTE POR GRUPO
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Obtiene calificaciones de todos los alumnos de un grupo específico
   */
  getReporteGrupo: function(matriculaProfesor, nombreGrupo) {
    console.log("📊 Generando reporte para grupo: " + nombreGrupo);
    
    try {
      var config = _getFirebaseConfig();
      var grupoKey = nombreGrupo.toUpperCase();
      
      var url = config.url + "calificaciones_por_profesor/" + matriculaProfesor + "/" + grupoKey + ".json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        return { ok: false, error: "Sin datos para este grupo" };
      }
      
      var datosGrupo = JSON.parse(response.getContentText());
      
      return {
        ok: true,
        grupo: nombreGrupo,
        materias: Object.keys(datosGrupo),
        datos: datosGrupo
      };
      
    } catch (e) {
      console.error("❌ Error en getReporteGrupo: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 3. OBTENER CALIFICACIONES DE UN ALUMNO
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Obtiene todas las calificaciones de un alumno específico
   * Retorna: promedio general, calificaciones por materia, estado
   */
  getCalificacionesAlumno: function(correoAlumno) {
    console.log("📊 Obteniendo calificaciones del alumno: " + correoAlumno);
    
    try {
      var config = _getFirebaseConfig();
      var correoKey = _sanitizarClave(correoAlumno);
      
      var url = config.url + "indices_por_alumno/" + correoKey + ".json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        return { ok: false, error: "Sin calificaciones para este alumno" };
      }
      
      var datosAlumno = JSON.parse(response.getContentText());
      
      // Calcular promedios
      var calificaciones = [];
      var sumaCalificaciones = 0;
      var conteo = 0;
      
      for (var key in datosAlumno) {
        if (datosAlumno.hasOwnProperty(key)) {
          var calif = datosAlumno[key];
          calificaciones.push(calif);
          sumaCalificaciones += parseFloat(calif.calificacion) || 0;
          conteo++;
        }
      }
      
      var promedio = conteo > 0 ? sumaCalificaciones / conteo : 0;
      var estado = Reportes._obtenerEstadoAlumno(promedio);
      
      return {
        ok: true,
        correoAlumno: correoAlumno,
        promedio: parseFloat(promedio.toFixed(2)),
        estado: estado,
        totalCalificaciones: conteo,
        calificaciones: calificaciones
      };
      
    } catch (e) {
      console.error("❌ Error en getCalificacionesAlumno: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 4. ALUMNOS EN RIESGO (Críticos + Bajos)
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Obtiene lista de alumnos con calificaciones bajas o críticas
   * Útil para identificar quiénes necesitan apoyo
   */
  getAlumnosEnRiesgo: function(matriculaProfesor, umbralRiesgo) {
    umbralRiesgo = umbralRiesgo || 6.0;
    
    console.log("⚠️ Buscando alumnos con promedio < " + umbralRiesgo);
    
    try {
      var reporte = Reportes.getReporteCompletoProfesor(matriculaProfesor);
      
      if (!reporte.ok) {
        return { ok: false, error: reporte.error };
      }
      
      var alumnosEnRiesgo = [];
      
      // Recorrer todos los grupos y encontrar alumnos críticos
      for (var grupo in reporte.grupos) {
        for (var materia in reporte.grupos[grupo].materias) {
          var alumnosMateria = reporte.grupos[grupo].materias[materia];
          
          for (var alumno in alumnosMateria) {
            var promedio = alumnosMateria[alumno].promedio;
            
            if (promedio < umbralRiesgo) {
              alumnosEnRiesgo.push({
                alumno: alumnosMateria[alumno].nombre,
                correo: alumno,
                grupo: grupo,
                materia: materia,
                promedio: promedio,
                estado: Reportes._obtenerEstadoAlumno(promedio)
              });
            }
          }
        }
      }
      
      // Ordenar por promedio (peor primero)
      alumnosEnRiesgo.sort(function(a, b) {
        return a.promedio - b.promedio;
      });
      
      return {
        ok: true,
        cantidad: alumnosEnRiesgo.length,
        umbral: umbralRiesgo,
        alumnos: alumnosEnRiesgo
      };
      
    } catch (e) {
      console.error("❌ Error en getAlumnosEnRiesgo: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 5. ESTADÍSTICAS POR MATERIA
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Obtiene estadísticas completas de una materia:
   * - Promedio general de la materia
   * - Distribución de calificaciones
   * - Alumnos destacados vs alumnos en riesgo
   */
  getEstadisticasMateria: function(matriculaProfesor, nombreMateria) {
    console.log("📊 Generando estadísticas para materia: " + nombreMateria);
    
    try {
      var reporte = Reportes.getReporteCompletoProfesor(matriculaProfesor);
      
      if (!reporte.ok) {
        return { ok: false, error: reporte.error };
      }
      
      var calificacionesMateria = [];
      var alumnosMateria = [];
      
      // Buscar todas las calificaciones de la materia
      for (var grupo in reporte.grupos) {
        for (var materia in reporte.grupos[grupo].materias) {
          if (materia.toUpperCase() === nombreMateria.toUpperCase()) {
            var alumnosGrupo = reporte.grupos[grupo].materias[materia];
            
            for (var alumno in alumnosGrupo) {
              var promedioAlumno = alumnosGrupo[alumno].promedio;
              calificacionesMateria.push(promedioAlumno);
              
              alumnosMateria.push({
                nombre: alumnosGrupo[alumno].nombre,
                correo: alumno,
                promedio: promedioAlumno,
                grupo: grupo
              });
            }
          }
        }
      }
      
      if (calificacionesMateria.length === 0) {
        return { ok: false, error: "Sin calificaciones para esta materia" };
      }
      
      // Calcular estadísticas
      var suma = calificacionesMateria.reduce(function(a, b) { return a + b; }, 0);
      var promedio = suma / calificacionesMateria.length;
      
      calificacionesMateria.sort(function(a, b) { return a - b; });
      
      var minimo = calificacionesMateria[0];
      var maximo = calificacionesMateria[calificacionesMateria.length - 1];
      var mediana = calificacionesMateria[Math.floor(calificacionesMateria.length / 2)];
      
      return {
        ok: true,
        materia: nombreMateria,
        totalAlumnos: alumnosMateria.length,
        promedio: parseFloat(promedio.toFixed(2)),
        minimo: minimo,
        maximo: maximo,
        mediana: mediana,
        alumnos: alumnosMateria
      };
      
    } catch (e) {
      console.error("❌ Error en getEstadisticasMateria: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 6. HELPERS PRIVADOS
  // ════════════════════════════════════════════════════════════════
  
  _procesarDatos: function(datos) {
    var resultado = {};
    
    for (var grupo in datos) {
      resultado[grupo] = {
        materias: {}
      };
      
      for (var materia in datos[grupo]) {
        var alumnosMateria = {};
        
        for (var alumno in datos[grupo][materia]) {
          var calificaciones = datos[grupo][materia][alumno].calificaciones || [];
          var suma = 0;
          
          calificaciones.forEach(function(c) {
            suma += parseFloat(c.calificacion) || 0;
          });
          
          var promedio = calificaciones.length > 0 ? suma / calificaciones.length : 0;
          
          alumnosMateria[alumno] = {
            nombre: datos[grupo][materia][alumno].nombre,
            promedio: parseFloat(promedio.toFixed(2)),
            totalCalificaciones: calificaciones.length
          };
        }
        
        resultado[grupo].materias[materia] = alumnosMateria;
      }
    }
    
    return resultado;
  },
  
  _contarAlumnosUnicos: function(datos) {
    var set = new Set();
    for (var grupo in datos) {
      for (var materia in datos[grupo]) {
        for (var alumno in datos[grupo][materia]) {
          set.add(datos[grupo][materia][alumno].correo);
        }
      }
    }
    return set.size;
  },
  
  _contarCalificacionesTotales: function(datos) {
    var total = 0;
    for (var grupo in datos) {
      for (var materia in datos[grupo]) {
        for (var alumno in datos[grupo][materia]) {
          total += (datos[grupo][materia][alumno].calificaciones || []).length;
        }
      }
    }
    return total;
  },
  
  _obtenerEstadoAlumno: function(promedio) {
    promedio = parseFloat(promedio) || 0;
    
    if (promedio >= 9) {
      return { emoji: "😎", texto: "Excelente", color: "#3B82F6" };
    }
    if (promedio >= 8) {
      return { emoji: "🙂", texto: "Satisfactorio", color: "#10B981" };
    }
    if (promedio >= 6) {
      return { emoji: "⚠️", texto: "En Riesgo", color: "#F59E0B" };
    }
    return { emoji: "😓", texto: "Crítico", color: "#EF4444" };
  }
  
};
