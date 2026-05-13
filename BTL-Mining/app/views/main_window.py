from __future__ import annotations

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from PyQt6.QtCore import QDate, Qt
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateEdit,
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.controllers.decision_controller import DecisionController
from app.controllers.outcome_controller import OutcomeController
from app.controllers.predict_controller import PredictController
from app.controllers.retrain_controller import RetrainController
from app.models.entities import ApplicationInput, OutcomeInput


class ProbabilityChart(QWidget):
    def __init__(self) -> None:
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.figure = Figure(figsize=(3, 2.2), dpi=100)
        self.figure.patch.set_facecolor("#F8FAFC")
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setStyleSheet("background: transparent;")
        layout.addWidget(self.canvas)
        self.update_chart(0)

    def update_chart(self, bad_probability: float) -> None:
        good_probability = max(0.0, 1.0 - bad_probability)
        self.figure.clear()
        ax = self.figure.add_subplot(111)
        ax.set_facecolor("#F8FAFC")

        labels = ["Trả tốt", "Nợ xấu"]
        values = [good_probability * 100.0, bad_probability * 100.0]
        colors = ["#0D9488", "#DC2626"]
        edge_colors = ["#0A7A72", "#B91C1C"]

        bars = ax.bar(labels, values, color=colors, width=0.45,
                      edgecolor=edge_colors, linewidth=1.2)

        ax.set_ylim(0, 120)
        ax.set_ylabel("Tỷ lệ (%)", fontsize=9, color="#475569", labelpad=6)
        ax.set_title("Đánh giá rủi ro", fontsize=10,
                     fontweight="bold", color="#0F172A", pad=8)

        for bar, value, color in zip(bars, values, colors):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    value + 3, f"{value:.1f}%",
                    ha="center", va="bottom",
                    fontsize=10, fontweight="bold", color=color)

        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#E2E8F0")
        ax.spines["bottom"].set_color("#E2E8F0")
        ax.tick_params(colors="#64748B", labelsize=9)
        ax.yaxis.grid(True, color="#E2E8F0", linewidth=0.8, linestyle="--")
        ax.set_axisbelow(True)

        self.figure.tight_layout(pad=1.2)
        self.canvas.draw_idle()


class MainWindow(QMainWindow):
    def __init__(
        self,
        predict_controller: PredictController,
        decision_controller: DecisionController,
        outcome_controller: OutcomeController,
        retrain_controller: RetrainController,
    ) -> None:
        super().__init__()
        self.predict_controller = predict_controller
        self.decision_controller = decision_controller
        self.outcome_controller = outcome_controller
        self.retrain_controller = retrain_controller
        self.current_application_id: int | None = None

        self.setWindowTitle("Hệ thống đánh giá rủi ro tín dụng")
        self.setMinimumSize(960, 640)
        self.resize(1100, 700)
        self._init_ui()
        self._apply_styles()

    # ── Stylesheet ─────────────────────────────────────────────────────────

    def _apply_styles(self) -> None:
        self.setStyleSheet("""
            QMainWindow, QWidget {
                background-color: #FFFFFF;
                font-family: "Segoe UI", "Tahoma", sans-serif;
                font-size: 13px;
                color: #111111;
            }
            QTabWidget::pane {
                border: none;
                background: #FFFFFF;
            }
            QTabBar::tab {
                background: #EDEDED;
                color: #111111;
                border: none;
                padding: 9px 16px;
                margin-right: 2px;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
                font-weight: 600;
                font-size: 13px;
            }
            QTabBar::tab:selected {
                background: #FFFFFF;
                color: #111111;
                border-bottom: 2px solid #444444;
            }
            QTabBar::tab:hover:!selected {
                background: #DDDDDD;
            }
            QGroupBox {
                background: #FFFFFF;
                border: 1px solid #CFCFCF;
                border-radius: 8px;
                margin-top: 10px;
                padding: 12px 10px 8px 10px;
                font-weight: 600;
                font-size: 13px;
                color: #111111;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                subcontrol-position: top left;
                left: 10px;
                top: -2px;
                padding: 0 4px;
                background: #FFFFFF;
                color: #111111;
            }
            QLineEdit, QSpinBox, QComboBox, QTextEdit, QDateEdit {
                background: #FFFFFF;
                border: 1px solid #BDBDBD;
                border-radius: 4px;
                padding: 5px 7px;
                color: #111111;
                font-size: 12px;
                selection-background-color: #D9D9D9;
                selection-color: #111111;
                min-height: 26px;
            }
            QLineEdit:focus, QSpinBox:focus, QComboBox:focus,
            QTextEdit:focus, QDateEdit:focus {
                border: 1px solid #444444;
            }
            QSpinBox::up-button, QSpinBox::down-button,
            QDateEdit::up-button, QDateEdit::down-button {
                width: 20px;
                background: #F0F0F0;
                border-left: 1px solid #BDBDBD;
            }
            QSpinBox::up-button:hover, QSpinBox::down-button:hover,
            QDateEdit::up-button:hover, QDateEdit::down-button:hover {
                background: #E0E0E0;
            }
            QSpinBox::up-arrow, QDateEdit::up-arrow {
                image: url("app/views/assets/spin_up.svg");
                width: 9px; height: 9px;
            }
            QSpinBox::down-arrow, QDateEdit::down-arrow {
                image: url("app/views/assets/spin_down.svg");
                width: 9px; height: 9px;
            }
            QComboBox::drop-down { border: none; width: 18px; padding-right: 2px; }
            QComboBox QAbstractItemView {
                background: #FFFFFF;
                border: 1px solid #BDBDBD;
                border-radius: 4px;
                selection-background-color: #D9D9D9;
                selection-color: #111111;
                padding: 2px;
                outline: none;
            }
            QComboBox QAbstractItemView::item { padding: 5px 8px; color: #111111; }
            QComboBox QAbstractItemView::item:selected { background: #D9D9D9; }
            QPushButton {
                background: #EDEDED;
                color: #111111;
                border: 1px solid #BDBDBD;
                border-radius: 4px;
                padding: 7px 12px;
                font-weight: 600;
                font-size: 12px;
                min-height: 32px;
            }
            QPushButton:hover { background: #E3E3E3; }
            QPushButton:pressed { background: #D9D9D9; }
            QProgressBar {
                border: 1px solid #BDBDBD;
                border-radius: 4px;
                background: #F5F5F5;
                text-align: center;
                color: #111111;
                font-weight: 600;
                font-size: 11px;
                min-height: 20px;
                max-height: 20px;
            }
            QProgressBar::chunk { border-radius: 3px; background: #7A7A7A; }
            QFrame#resultPanel {
                background: #F8F9FA;
                border: 1px solid #CFCFCF;
                border-radius: 8px;
            }
            QTextEdit#noteBox {
                background: #FFFFFF;
                border: 1px solid #BDBDBD;
                border-radius: 4px;
                color: #555555;
                font-size: 11px;
                padding: 6px;
            }
            QTextEdit#retrainLog {
                background: #FAFAFA;
                color: #111111;
                border: 1px solid #BDBDBD;
                border-radius: 4px;
                font-family: "Consolas", monospace;
                font-size: 12px;
                padding: 8px;
            }
            QCheckBox {
                spacing: 8px;
                font-size: 13px;
                color: #111111;
                font-weight: 500;
            }
            QCheckBox::indicator {
                width: 15px; height: 15px;
                border-radius: 3px;
                border: 1px solid #8A8A8A;
                background: #FFFFFF;
            }
            QCheckBox::indicator:checked {
                background: #444444;
                border-color: #444444;
            }
            QFormLayout QLabel {
                color: #333333;
                font-size: 12px;
                font-weight: 500;
            }
            QScrollArea { border: none; background: transparent; }
            QScrollBar:vertical {
                background: #F5F5F5; width: 7px; border-radius: 4px;
            }
            QScrollBar::handle:vertical {
                background: #C0C0C0; border-radius: 4px;
                min-height: 20px; margin: 2px;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0px; }
        """)

    # ── UI Construction ────────────────────────────────────────────────────

    def _init_ui(self) -> None:
        root = QWidget()
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(10, 10, 10, 10)
        root_layout.setSpacing(8)

        # Header
        header = QFrame()
        header.setStyleSheet("""
            QFrame {
                background: #F5F5F5;
                border: 1px solid #CFCFCF;
                border-radius: 8px;
            }
        """)
        hl = QHBoxLayout(header)
        hl.setContentsMargins(14, 10, 14, 10)

        title = QLabel("Hệ thống đánh giá rủi ro tín dụng")
        title.setStyleSheet(
            "font-size: 17px; font-weight: 700; color: #111111; background: transparent;")
        hl.addWidget(title)
        hl.addStretch()

        root_layout.addWidget(header)

        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        self.tabs.addTab(self._build_approval_tab(),  "Xét duyệt khách hàng")
        self.tabs.addTab(self._build_outcome_tab(),   "Cập nhật trả nợ")
        self.tabs.addTab(self._build_retrain_tab(),   "Học từ dữ liệu mới")
        root_layout.addWidget(self.tabs)

        self.setCentralWidget(root)

    def _build_approval_tab(self) -> QWidget:
        page = QWidget()
        page.setStyleSheet("background: #FFFFFF;")
        outer = QHBoxLayout(page)
        outer.setContentsMargins(10, 10, 10, 10)
        outer.setSpacing(12)

        self.input_widgets: dict[str, QComboBox | QSpinBox] = {}

        # ── Trái: Scroll area chứa toàn bộ form ──
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        form_container = QWidget()
        form_container.setStyleSheet("background: #FFFFFF;")
        form_layout = QVBoxLayout(form_container)
        form_layout.setContentsMargins(4, 4, 8, 4)
        form_layout.setSpacing(10)

        # ── Row 1: Thông tin cơ bản + Thông tin khoản vay ──
        row1 = QHBoxLayout()
        row1.setSpacing(10)

        basic_group = QGroupBox("Thông tin cơ bản")
        bf = QFormLayout(basic_group)
        bf.setSpacing(6)
        bf.setLabelAlignment(Qt.AlignmentFlag.AlignRight)
        bf.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        self._add_combo(bf, "status", "Tài khoản",
            [("A11","Dưới 0 DM"),("A12","0–200 DM"),("A13","Từ 200 DM+"),("A14","Không có")])
        self._add_spin(bf, "age", "Tuổi", 18, 80, 35)
        self._add_combo(bf, "employment", "Thâm niên",
            [("A71","Thất nghiệp"),("A72","< 1 năm"),("A73","1–4 năm"),("A74","4–7 năm"),("A75","> 7 năm")])
        self._add_combo(bf, "personal_status", "Tình trạng",
            [("A91","Nam — ly hôn"),("A92","Nữ — kết hôn"),("A93","Nam — độc thân"),
             ("A94","Nam — kết hôn"),("A95","Nữ — độc thân")])
        self._add_combo(bf, "housing", "Nhà ở",
            [("A151","Thuê"),("A152","Sở hữu"),("A153","Miễn phí")])
        row1.addWidget(basic_group)

        finance_group = QGroupBox("Thông tin khoản vay")
        ff = QFormLayout(finance_group)
        ff.setSpacing(6)
        ff.setLabelAlignment(Qt.AlignmentFlag.AlignRight)
        ff.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        self._add_spin(ff, "credit_amount", "Số tiền (DM)", 250, 50000, 2500)
        self._add_spin(ff, "duration", "Thời hạn (tháng)", 4, 72, 12)
        self._add_spin(ff, "installment_rate", "Tỷ lệ trả góp (%)", 1, 4, 2)
        self._add_combo(ff, "purpose", "Mục đích",
            [("A40","Xe mới"),("A41","Xe cũ"),("A42","Nội thất"),("A43","Điện tử"),
             ("A44","Gia dụng"),("A45","Sửa chữa"),("A46","Giáo dục"),("A48","Đào tạo"),
             ("A49","Kinh doanh"),("A410","Khác")])
        self._add_combo(ff, "credit_history", "Lịch sử tín dụng",
            [("A30","Luôn đúng hạn"),("A31","Đã từng vay"),("A32","Đang trả tốt"),
             ("A33","Từng trễ hạn"),("A34","Rủi ro cao")])
        self._add_combo(ff, "savings", "Tiết kiệm",
            [("A61","< 100 DM"),("A62","100–500 DM"),("A63","500–1.000 DM"),
             ("A64","> 1.000 DM"),("A65","Không rõ")])
        row1.addWidget(finance_group)

        form_layout.addLayout(row1)

        # ── Row 2: Thông tin bổ sung (dạng 2 cột nội bộ) ──
        extra_group = QGroupBox("Thông tin bổ sung")
        eg = QHBoxLayout(extra_group)
        eg.setContentsMargins(8, 8, 8, 8)
        eg.setSpacing(16)

        # Cột trái — bọc trong QWidget để QFormLayout render đúng
        left_w = QWidget()
        left_w.setStyleSheet("background: transparent;")
        ef_left = QFormLayout(left_w)
        ef_left.setContentsMargins(0, 0, 0, 0)
        ef_left.setSpacing(7)
        ef_left.setLabelAlignment(Qt.AlignmentFlag.AlignRight)
        ef_left.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        self._add_combo(ef_left, "other_debtors", "Bảo lãnh",
            [("A101","Không có"),("A102","Đồng vay"),("A103","Người bảo lãnh")])
        self._add_spin(ef_left, "residence_since", "Số năm cư trú", 1, 10, 2)
        self._add_combo(ef_left, "property", "Tài sản",
            [("A121","Bất động sản"),("A122","Bảo hiểm nhân thọ"),("A123","Xe / tài sản khác"),("A124","Không rõ")])
        self._add_combo(ef_left, "other_installment_plans", "Trả góp nơi khác",
            [("A141","Ngân hàng khác"),("A142","Cửa hàng"),("A143","Không có")])
        eg.addWidget(left_w, stretch=1)

        # Divider dọc
        vline = QFrame()
        vline.setFrameShape(QFrame.Shape.VLine)
        vline.setStyleSheet("color: #E0E0E0;")
        eg.addWidget(vline)

        # Cột phải — bọc trong QWidget
        right_w = QWidget()
        right_w.setStyleSheet("background: transparent;")
        ef_right = QFormLayout(right_w)
        ef_right.setContentsMargins(0, 0, 0, 0)
        ef_right.setSpacing(7)
        ef_right.setLabelAlignment(Qt.AlignmentFlag.AlignRight)
        ef_right.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        self._add_spin(ef_right, "existing_credits", "Khoản vay hiện có", 1, 6, 1)
        self._add_combo(ef_right, "job", "Nghề nghiệp",
            [("A171","Không ổn định"),("A172","Lao động phổ thông"),("A173","Có tay nghề"),("A174","Quản lý / Chuyên gia")])
        self._add_spin(ef_right, "people_liable", "Người phụ thuộc", 1, 4, 1)
        self._add_combo(ef_right, "telephone", "Điện thoại", [("A191","Không có"),("A192","Có đăng ký")])
        self._add_combo(ef_right, "foreign_worker", "Lao động nước ngoài", [("A201","Có"),("A202","Không")])
        eg.addWidget(right_w, stretch=1)

        form_layout.addWidget(extra_group)
        form_layout.addStretch()

        scroll.setWidget(form_container)
        outer.addWidget(scroll, stretch=1)

        # ── Phải: Panel kết quả cố định ──
        result_panel = QFrame()
        result_panel.setObjectName("resultPanel")
        result_panel.setFixedWidth(300)
        rp_layout = QVBoxLayout(result_panel)
        rp_layout.setContentsMargins(16, 16, 16, 16)
        rp_layout.setSpacing(0)  # Spacing thủ công bằng addSpacing

        # App ID
        self.app_id_label = QLabel("Mã hồ sơ: —")
        self.app_id_label.setStyleSheet(
            "font-size: 12px; font-weight: 700; color: #888888; background: transparent;")
        rp_layout.addWidget(self.app_id_label)
        rp_layout.addSpacing(10)

        div = QFrame()
        div.setFrameShape(QFrame.Shape.HLine)
        div.setStyleSheet("color: #E0E0E0;")
        rp_layout.addWidget(div)
        rp_layout.addSpacing(12)

        # Khuyến nghị
        lbl_rec = QLabel("KHUYẾN NGHỊ")
        lbl_rec.setStyleSheet(
            "font-size: 10px; font-weight: 700; color: #AAAAAA; "
            "letter-spacing: 1.5px; background: transparent;")
        rp_layout.addWidget(lbl_rec)
        rp_layout.addSpacing(4)

        self.recommendation_label = QLabel("Chưa có dữ liệu")
        self.recommendation_label.setStyleSheet(
            "font-size: 22px; font-weight: 800; color: #888888; background: transparent;")
        rp_layout.addWidget(self.recommendation_label)
        rp_layout.addSpacing(14)

        # Điểm rủi ro
        lbl_risk = QLabel("ĐIỂM RỦI RO")
        lbl_risk.setStyleSheet(
            "font-size: 10px; font-weight: 700; color: #AAAAAA; "
            "letter-spacing: 1.5px; background: transparent;")
        rp_layout.addWidget(lbl_risk)
        rp_layout.addSpacing(4)

        self.risk_progress = QProgressBar()
        self.risk_progress.setRange(0, 100)
        self.risk_progress.setValue(0)
        self.risk_progress.setFormat("%p / 100")
        rp_layout.addWidget(self.risk_progress)
        rp_layout.addSpacing(14)

        # Biểu đồ
        self.chart = ProbabilityChart()
        self.chart.setFixedHeight(210)
        rp_layout.addWidget(self.chart)
        rp_layout.addSpacing(10)

        # Ghi chú
        self.note_box = QTextEdit()
        self.note_box.setObjectName("noteBox")
        self.note_box.setReadOnly(True)
        self.note_box.setFixedHeight(62)
        self.note_box.setPlainText(
            "Hệ thống AI chỉ hỗ trợ tham khảo.\nNhân viên có quyền quyết định cuối cùng.")
        rp_layout.addWidget(self.note_box)

        rp_layout.addStretch()

        # Nút thao tác
        analyze_btn = QPushButton("Phân tích hồ sơ")
        analyze_btn.setObjectName("analyzeBtn")
        analyze_btn.setFixedHeight(36)
        analyze_btn.clicked.connect(self.on_predict_clicked)
        rp_layout.addWidget(analyze_btn)
        rp_layout.addSpacing(10)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(8)
        approve_btn = QPushButton("✓  Duyệt vay")
        approve_btn.setObjectName("approveBtn")
        approve_btn.setStyleSheet("""
            QPushButton {
                background: #DC2626;
                color: #FFFFFF;
                border: 1px solid #B91C1C;
                border-radius: 4px;
                padding: 7px 12px;
                font-weight: 600;
                font-size: 12px;
                min-height: 32px;
            }
            QPushButton:hover { background: #B91C1C; }
            QPushButton:pressed { background: #991B1B; }
        """)
        approve_btn.clicked.connect(lambda: self._save_decision_from_approval(1))

        reject_btn = QPushButton("✕  Từ chối")
        reject_btn.setObjectName("rejectBtn")
        reject_btn.setStyleSheet("""
            QPushButton {
                background: #0D9488;
                color: #FFFFFF;
                border: 1px solid #0A7A72;
                border-radius: 4px;
                padding: 7px 12px;
                font-weight: 600;
                font-size: 12px;
                min-height: 32px;
            }
            QPushButton:hover { background: #0A7A72; }
            QPushButton:pressed { background: #0F766E; }
        """)
        reject_btn.clicked.connect(lambda: self._save_decision_from_approval(0))
        btn_row.addWidget(approve_btn)
        btn_row.addWidget(reject_btn)
        rp_layout.addLayout(btn_row)

        outer.addWidget(result_panel)
        return page

    def _build_outcome_tab(self) -> QWidget:
        page = QWidget()
        page.setStyleSheet("background: #FFFFFF;")
        outer = QVBoxLayout(page)
        outer.setContentsMargins(24, 20, 24, 20)
        outer.setSpacing(16)

        info = QLabel(
            "Cập nhật kết quả thực tế sau khi khoản vay đến hạn "
            "để hệ thống học thêm từ dữ liệu mới.")
        info.setStyleSheet(
            "font-size: 13px; color: #111111; font-weight: 500; "
            "background: #F5F5F5; border-radius: 6px; padding: 10px 12px; "
            "border: 1px solid #CFCFCF;")
        info.setWordWrap(True)
        outer.addWidget(info)

        card = QGroupBox("Chọn hồ sơ & Cập nhật kết quả trả nợ")
        form = QFormLayout(card)
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignRight)

        self.outcome_app_id = QComboBox()
        self.outcome_app_id.currentIndexChanged.connect(self._on_outcome_selected)
        self._load_pending_outcomes()

        self.outcome_due_date = QDateEdit()
        self.outcome_due_date.setCalendarPopup(True)
        self.outcome_due_date.setDate(QDate.currentDate())

        self.outcome_paid = QCheckBox("Khách hàng đã thanh toán đầy đủ")

        self.outcome_paid_date = QDateEdit()
        self.outcome_paid_date.setCalendarPopup(True)
        self.outcome_paid_date.setDate(QDate.currentDate())

        self.outcome_actual = QComboBox()
        self.outcome_actual.addItems(["Trả tốt (0)", "Nợ xấu / trễ hạn (1)"])

        save_btn = QPushButton("Lưu kết quả trả nợ")
        save_btn.setFixedHeight(40)
        save_btn.clicked.connect(self.on_save_outcome)

        form.addRow("Hồ sơ chưa trả nợ", self.outcome_app_id)
        form.addRow("Ngày đến hạn", self.outcome_due_date)
        form.addRow("Trạng thái thanh toán", self.outcome_paid)
        form.addRow("Ngày thanh toán", self.outcome_paid_date)
        form.addRow("Kết quả thực tế", self.outcome_actual)
        form.addRow("", save_btn)

        outer.addWidget(card)
        outer.addStretch()
        return page

    def _build_retrain_tab(self) -> QWidget:
        page = QWidget()
        page.setStyleSheet("background: #FFFFFF;")
        layout = QVBoxLayout(page)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(12)

        note = QLabel(
            "Khi đã tích lũy đủ dữ liệu phản hồi thực tế từ tab 'Cập nhật trả nợ', "
            "nhấn nút bên dưới để huấn luyện lại mô hình — "
            "giúp dự đoán sát thực tế hơn theo thời gian.")
        note.setStyleSheet(
            "font-size: 13px; color: #111111; font-weight: 500; "
            "background: #F5F5F5; border-radius: 6px; padding: 10px 12px; "
            "border: 1px solid #CFCFCF;")
        note.setWordWrap(True)
        layout.addWidget(note)

        run_btn = QPushButton("Cập nhật lại mô hình")
        run_btn.setObjectName("retrainBtn")
        run_btn.setFixedHeight(42)
        run_btn.clicked.connect(self.on_retrain)
        layout.addWidget(run_btn)

        log_label = QLabel("Nhật ký huấn luyện")
        log_label.setStyleSheet(
            "font-size: 13px; font-weight: 700; color: #333333; margin-top: 6px;")
        layout.addWidget(log_label)

        self.retrain_output = QTextEdit()
        self.retrain_output.setObjectName("retrainLog")
        self.retrain_output.setReadOnly(True)
        self.retrain_output.setPlainText("// Chờ kết quả huấn luyện...")
        layout.addWidget(self.retrain_output)

        return page

    # ── Helpers ────────────────────────────────────────────────────────────

    def _add_combo(self, form: QFormLayout, key: str, label: str, values: list[tuple[str, str]]) -> None:
        combo = QComboBox()
        for code, text in values:
            combo.addItem(text, userData=code)
        self.input_widgets[key] = combo
        form.addRow(label, combo)

    def _add_spin(self, form: QFormLayout, key: str, label: str, min_v: int, max_v: int, default: int) -> None:
        spin = QSpinBox()
        spin.setRange(min_v, max_v)
        spin.setValue(default)
        self.input_widgets[key] = spin
        form.addRow(label, spin)

    def _collect_application(self) -> ApplicationInput:
        def combo_val(key: str) -> str:
            widget = self.input_widgets[key]
            if isinstance(widget, QComboBox):
                return str(widget.currentData()).strip()
            return str(widget.value()).strip()

        return ApplicationInput(
            status=combo_val("status"),
            duration=int(combo_val("duration")),
            credit_history=combo_val("credit_history"),
            purpose=combo_val("purpose"),
            credit_amount=int(combo_val("credit_amount")),
            savings=combo_val("savings"),
            employment=combo_val("employment"),
            installment_rate=int(combo_val("installment_rate")),
            personal_status=combo_val("personal_status"),
            other_debtors=combo_val("other_debtors"),
            residence_since=int(combo_val("residence_since")),
            property=combo_val("property"),
            age=int(combo_val("age")),
            other_installment_plans=combo_val("other_installment_plans"),
            housing=combo_val("housing"),
            existing_credits=int(combo_val("existing_credits")),
            job=combo_val("job"),
            people_liable=int(combo_val("people_liable")),
            telephone=combo_val("telephone"),
            foreign_worker=combo_val("foreign_worker"),
        )

    # ── Slots ──────────────────────────────────────────────────────────────

    def on_predict_clicked(self) -> None:
        try:
            app_input = self._collect_application()
            app_id, result = self.predict_controller.predict_and_store(app_input, source="SYSTEM")
            self.current_application_id = app_id
            self._load_pending_outcomes()
            for i in range(self.outcome_app_id.count()):
                if self.outcome_app_id.itemData(i) == app_id:
                    self.outcome_app_id.setCurrentIndex(i)
                    break

            suggestion = "Từ chối" if result.auto_decision == 0 else "Duyệt"

            self.app_id_label.setText(f"Mã hồ sơ: #{app_id}")
            self.recommendation_label.setText(suggestion)
            self.recommendation_label.setStyleSheet(
                "font-size: 22px; font-weight: 700; color: #111111; background: transparent;")
            self.risk_progress.setValue(int(result.risk_score))
            self.chart.update_chart(result.bad_probability)
            self.note_box.setPlainText(
                f"Xác suất nợ xấu:  {result.bad_probability:.2%}\n"
                f"Điểm rủi ro:         {result.risk_score:.2f} / 100\n"
                "Khuyến nghị chỉ mang tính hỗ trợ.")

            QMessageBox.information(self, "Phân tích hoàn tất",
                                    "Hồ sơ đã được phân tích và lưu thành công.")
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi phân tích", str(ex))

    def _save_decision_from_approval(self, decision: int) -> None:
        try:
            if self.current_application_id is None:
                QMessageBox.warning(self, "Thiếu dữ liệu", "Vui lòng phân tích hồ sơ trước khi ra quyết định.")
                return
            self.decision_controller.save_decision(self.current_application_id, decision)
            msg = "Đã duyệt hồ sơ khách hàng." if decision == 1 else "Đã từ chối hồ sơ khách hàng."
            QMessageBox.information(self, "Đã cập nhật", msg)
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi xét duyệt", str(ex))

    def _load_pending_outcomes(self) -> None:
        try:
            self.outcome_app_id.blockSignals(True)
            self.outcome_app_id.clear()
            pending_applications = self.outcome_controller.get_pending_outcomes()
            if not pending_applications:
                self.outcome_app_id.addItem("Không có hồ sơ chưa trả nợ", userData=None)
            else:
                for app_id, _, credit_amount in pending_applications:
                    amount_value = float(credit_amount) if credit_amount is not None else 0.0
                    amount_text = f"{amount_value:,.0f}".replace(",", ".")
                    display_text = f"Hồ sơ #{app_id} — {amount_text} DM"
                    self.outcome_app_id.addItem(display_text, userData=app_id)
            self.outcome_app_id.blockSignals(False)
        except Exception as ex:
            QMessageBox.warning(self, "Lỗi tải dữ liệu", f"Không thể tải danh sách hồ sơ: {str(ex)}")

    def _on_outcome_selected(self) -> None:
        app_id = self.outcome_app_id.currentData()
        if app_id is None:
            self.outcome_due_date.setDate(QDate.currentDate())
            self.outcome_paid.setChecked(False)
            self.outcome_paid_date.setDate(QDate.currentDate())
            self.outcome_actual.setCurrentIndex(0)

    def on_save_outcome(self) -> None:
        try:
            app_id = self.outcome_app_id.currentData()
            if app_id is None:
                QMessageBox.warning(self, "Lỗi", "Vui lòng chọn một hồ sơ chưa trả nợ.")
                return
            due_date = self.outcome_due_date.date().toPyDate()
            paid = self.outcome_paid.isChecked()
            paid_date = self.outcome_paid_date.date().toPyDate() if paid else None
            actual_outcome = 0 if self.outcome_actual.currentIndex() == 0 else 1

            outcome = OutcomeInput(
                application_id=app_id,
                due_date=due_date,
                paid=paid,
                paid_date=paid_date,
                actual_outcome=actual_outcome,
            )
            self.outcome_controller.save_outcome(outcome)
            QMessageBox.information(self, "Đã lưu", "Kết quả trả nợ đã được cập nhật thành công.")
            self._load_pending_outcomes()
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi cập nhật", str(ex))

    def on_retrain(self) -> None:
        try:
            metrics = self.retrain_controller.retrain()
            lines = [f"{key}: {value}" for key, value in metrics.items()]
            self.retrain_output.setPlainText("\n".join(lines))
            QMessageBox.information(self, "Thành công", "Mô hình đã được cập nhật.")
        except Exception as ex:
            QMessageBox.critical(self, "Lỗi cập nhật mô hình", str(ex))