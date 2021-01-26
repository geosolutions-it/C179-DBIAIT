const table_function_mapper = {
    "#import-status-table": function (response) {
        const table = $("#import-status-table");
        if (response) {
            table.empty();
            response.forEach(function (data) {
                table.append(`<tr class="${data.style_class}"><td id='task_uuid'>${data.uuid}</td><td><div class="progress"><div class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: ${data.progress}%;" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100">${data.progress}%</div></div></td><td class="text-lowercase">${data.status}</td><td><i class="${data.status_icon}"></i></td></tr>`);
            });
        }
    },
    "#freeze-status-table": function (response) {
        const table = $("#freeze-status-table");
        if (response) {
            table.empty();
            response.forEach(function (data) {
                table.append(`<tr class="${data.style_class}"><td id='task_uuid'>${data.uuid}</td><td><div class="progress"><div class="progress-bar progress-bar-striped progress-bar-animated" role="progressbar" style="width: ${data.progress}%;" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100">${data.progress}%</div></div></td><td class="text-lowercase">${data.status}</td><td><i class="${data.status_icon}"></i></td></tr>`);
            });
        }
    },
    "#test-import-table": function (response) {
        const table = $("#dt-import-table").DataTable();
        if (response) {
            table.clear().draw();
            response.forEach(function (data) {
                var row = table.row.add([
                    data.layer_name,
                    get_local_date(data.import_start_timestamp) ,
                    get_local_date(data.import_end_timestamp),
                    data.status
                    ]).draw( false ).node();
                $(row).addClass(data.style_class);
            });
        }
    },
    "#layer-freeze-table": function (response) {
        const table = $("#dt-freeze-table").DataTable();
        if (response) {
            table.clear().draw();
            response.forEach(function (data) {
                var row = table.row.add([
                    data.layer_name,
                    get_local_date(data.freeze_start_timestamp) ,
                    get_local_date(data.freeze_end_timestamp),
                    data.status
                    ]).draw( false ).node();
                $(row).addClass(data.style_class);
            });
        }
    },
    "#process-status-table": function (response) {
        const table = $("#processing-table").DataTable();
        if (response) {
            table.clear().draw();
            response.forEach(function (data) {
                var escaped = escape(data.task_log);
                var errorModal = '<a href="#" onclick="display_error_log(\'' + escaped + '\', \'' + data.status + '\')" data-toggle="modal" data-target=".bd-example-modal-lg"><i class="'+ data.status_icon + '"></i></a>'
                var row = table.row.add([
                    data.id,
                    data.user,
                    get_local_date(data.start_date)  || "--/--/---" ,
                    get_local_date(data.end_date)  || "--/--/---" ,
                    errorModal
                    ]).draw( false ).node();
                $(row).addClass(data.style_class);
            });
        }
    },
    "#export-status-table": function (response) {
        const table = $("#export-table").DataTable();
        if (response) {
            table.clear().draw();
            response.forEach(function (data) {
            var escaped = escape(data.task_log)
            if (escaped.length > 10000) {
                var escaped = escape(data.task_log).substr(0, 10000) + "......<br>Controlla il log nello zip per maggiori informazioni";
            }
            var errorModal = '<a href="#" onclick="display_error_log(\'' + escaped + '\', \'' + data.status + '\')" data-toggle="modal" data-target=".bd-example-modal-lg"><i class="'+ data.status_icon + '"></i></a>'
            if (data.status == 'RUNNING' || data.status == 'QUEUED') {
                download = ""
            } else {
                var download = '<a href="download/' + data.id + '" target="_blank"><i class="fas fa-download"></i></a>';
            }
            table.row.add([
                data.id,
                data.user,
                data.geopackage_name,
                get_local_date(data.start_date)  || "--/--/---" ,
                get_local_date(data.end_date)  || "--/--/---" ,
                errorModal,
                download,
                ]).draw( false );
            });
        }
    },
}

function ajax_call(url, table) {
    $.ajax({
        type: "GET",
        url: url,
        dataType: "json",
        success: table_function_mapper[table]
    });
}

function get_local_date(utc_date) {
    if (utc_date == null || utc_date == "None") {
        return "--/--/---"
    }
    else {
        const localDate = new Date(utc_date);
        return moment(localDate.toUTCString(), 'ddd, DD MMM YYYY HH:mm:ss').format("DD/MM/YYYY - HH:mm:ss");
    }
}

function changeTimeFormat(date) {
}

window.addEventListener("load", function () {
    $(function () {
        $('[data-toggle="tooltip"]').tooltip()
    });
});

const text_style_mapper = {
    "SUCCESS": "text-success",
    "FAILED": "text-danger"
}

function display_error_log(task_log, status){
    (task_log === "null") && $('#task-log').html("<div class='text-info'>This task has no logs</div>");
    (task_log != "null") && $('#task-log').html(`<div class="${text_style_mapper[status]}">${unescape(task_log)}</div>`);
}
